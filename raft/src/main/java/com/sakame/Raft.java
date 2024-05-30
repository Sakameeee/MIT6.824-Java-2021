package com.sakame;

import cn.hutool.core.util.ArrayUtil;
import com.sakame.constant.RaftConstant;
import com.sakame.model.Channel;
import com.sakame.model.Entry;
import com.sakame.model.RaftState;
import com.sakame.model.dto.*;
import com.sakame.service.RaftService;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * raft 类，非单例
 *
 * @author sakame
 * @version 1.0
 */
@Slf4j
public class Raft implements RaftService {

    private static final int ELECTION_TIMEOUT_MIN = 150;
    private static final int ELECTION_TIMEOUT_MAX = 400;
    private static final int HEARTBEAT_INTERVAL = 50;
    private final Random random = new Random();
    private RaftState state;

    /**
     * 初始化一个 raft，一个 raft 代表一个线程
     *
     * @param peers
     * @param me
     * @param persister
     * @param channel
     */
    public void init(RaftService[] peers, int me, Persister persister, Channel<ApplyMsg> channel) {
        int n = peers.length;
        state = new RaftState();
        // 初始化状态
        state.setPeers(peers);
        state.setPersister(persister);
        state.setMe(me);
        state.setReplicatorCond(new Condition[n]);
        state.setLogs(new Entry[]{new Entry()});
        state.setChannel(channel);
        resetElectionTimer();
        resetHeartbeatTimer();

        readPersist();
//        Entry lastLog = this.getLastLog();

//        for (int i = 0; i < n; i++) {
//            state.getNextIndex()[i] = lastLog.getIndex() + 1;
//            state.getMatchIndex()[i] = 0;
//            if (i != me) {
//                state.getReplicatorCond()[i] = state.getLock().newCondition();
//                int finalI = i;
//                // 开启状态复制线程
//                new Thread(() -> replicator(finalI));
//            }
//        }

        // 开启计时线程
        new Thread(this::ticker).start();
        // 开启应用提交线程
        new Thread(this::applier).start();
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        // 尝试获取锁,避免死锁,如果获取不到则说明对方也在发起选举
        boolean tried = state.getLock().tryLock();
        if (!tried) {
            log.info("fail to get lock of raft:{}", state.getMe());
            return null;
        }

        RequestVoteResponse response = new RequestVoteResponse();
        log.info("raft:{} receive RequestVoteRequest:{}", state.getMe(), request);
        log.info("raft:{} current state:term:{}, votedFor:{}, lastLog:{}", state.getMe(), state.getCurrentTerm(), state.getVotedFor(), getLastLog());

        if (request.getTerm() < state.getCurrentTerm()) {
            response.setVoteGranted(false);
            response.setTerm(state.getCurrentTerm());
            persist();
            state.getLock().unlock();
            return response;
        }

        if (request.getTerm() > state.getCurrentTerm()) {
            state.setCurrentTerm(request.getTerm());
            state.setVotedFor(-1);
            trunTo(RaftConstant.FOLLOWER);
        }

        if (state.getVotedFor() == -1 || state.getVotedFor() == request.getCandidateId()) {
            if (!isLogUpToDate(request.getLastLogIndex(), request.getLastLogTerm())) {
                response.setVoteGranted(false);
                response.setTerm(state.getCurrentTerm());
                persist();
                state.getLock().unlock();
                return response;
            }

            state.setVotedFor(request.getCandidateId());
            response.setVoteGranted(true);
            response.setTerm(state.getCurrentTerm());
            resetElectionTimer();
            persist();
            state.getLock().unlock();
            return response;
        }

        response.setVoteGranted(false);
        response.setTerm(state.getCurrentTerm());
        persist();
        state.getLock().unlock();
        return response;
    }

    @Override
    public void requestHeartbeat() {
        resetElectionTimer();
    }

    @Override
    public AppendEntriesResponse requestAppendEntries(AppendEntriesRequest request) {
        log.info("raft:{} receives appending request:{}", state.getMe(), request);
        state.getLock().lock();
        AppendEntriesResponse response = new AppendEntriesResponse();

        if (request.getTerm() < state.getCurrentTerm()) {
            response.setTerm(state.getCurrentTerm());
            response.setSucceeded(false);
            state.getLock().unlock();
            return response;
        }

        // 检查状态
        if (request.getTerm() > state.getCurrentTerm()) {
            state.setCurrentTerm(request.getTerm());
            state.setVotedFor(-1);
            trunTo(RaftConstant.FOLLOWER);
        }

        if (state.getState() != RaftConstant.FOLLOWER) {
            trunTo(RaftConstant.FOLLOWER);
        }

        response.setSucceeded(true);
        response.setTerm(state.getCurrentTerm());
        resetElectionTimer();
        log.info("raft:{} resets electionTimer", state.getMe());

        // 如果 leader 发来的检查点位置的日志的 term 与自身相同索引位置的 term 不一致
        // 说明该 follower 的日志并非最新
        // 寻找最旧的冲突日志索引位置（term 相同，index 最小，设置 response 的 conflict 参数）
        int index = transfer(request.getPreLogIndex());
        Entry[] logs = state.getLogs();
        log.info("raft:{}, preLogIndex:{}, transferIndex:{}, logs:{}", state.getMe(), request.getPreLogIndex(), index, logs);
        if (index != -1 && logs[index].getTerm() != request.getPreLogTerm()) {
            log.info("raft:{}: a conflict occurred at index {}", state.getMe(), index);
            response.setSucceeded(false);
            response.setConflictTerm(logs[index].getTerm());
            response.setConflictIndex(request.getPreLogIndex());
            // catch up quickly
            for (int i = index; i >= 1; i--) {
                if (logs[i - 1].getTerm() != response.getConflictTerm()) {
                    response.setConflictTerm(logs[i].getTerm());
                    break;
                }
            }
            state.getLock().unlock();
            return response;
        }

        // 通过上面的冲突位置检查则说明已经找到日志出现分叉的索引位置
        // 开始日志复制
        if (request.getEntries() != null && request.getEntries().length != 0) {
            if (isConflict(request)) {
                state.setLogs(ArrayUtil.sub(state.getLogs(), 0, index + 1));
                state.setLogs(ArrayUtil.append(state.getLogs(), request.getEntries()));
                log.info("raft:{}: truncate logs at index:{}", state.getMe(), index);
                log.info("raft:{} appended logs:{}", state.getMe(), state.getLogs());
            }
        } else {
            log.info("raft:{}: length of entries is zero", state.getMe());
        }

        // 检查提交位置是否一致
        if (request.getLeaderCommit() > state.getCommitIndex()) {
            state.setCommitIndex(request.getLeaderCommit());
            if (request.getLeaderCommit() > getLastLog().getIndex()) {
                state.setCommitIndex(getLastLog().getIndex());
            }
            log.info("raft:{} commit to index:{}(lastLogIndex:{})", state.getMe(), state.getCommitIndex(), getLastLog().getIndex());
            state.getApplyCond().signal();
        }

        persist();
        state.getLock().unlock();
        return response;
    }

    /**
     * 开启一轮选举
     */
    public void doElection() {
        RequestVoteRequest request = genRequest();

        int n = state.getPeers().length;
        int votes = 1;
        CompletableFuture<Integer>[] tasks = new CompletableFuture[n];
        ReentrantLock lock = new ReentrantLock();
        for (int i = 0; i < n; i++) {
            if (i == state.getMe()) {
                tasks[i] = CompletableFuture.supplyAsync(() -> 0);
                continue;
            }

            int peer = i;
            tasks[i] = CompletableFuture.supplyAsync(() -> {
                RequestVoteResponse response = sendRequestVote(peer, request);

                if (response == null) {
                    return 0;
                }

                lock.lock();

                if (response.getTerm() == state.getCurrentTerm() && state.getState() == RaftConstant.CANDIDATE) {
                    if (response.isVoteGranted()) {
                        lock.unlock();
                        return 1;
                    } else if (response.getTerm() > state.getCurrentTerm()) {
                        state.setCurrentTerm(response.getTerm());
                        state.setVotedFor(-1);
                        trunTo(RaftConstant.FOLLOWER);
                        log.info("Node:{} finds a new leader:{} with term:{}", state.getMe(), request.getTerm(), state.getCurrentTerm());
                        persist();
                    }
                }

                lock.unlock();
                return 0;
            });
        }

        // 异步进行
        CompletableFuture.allOf(tasks).join();
        for (int i = 0; i < n; i++) {
            try {
                votes += tasks[i].get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (votes > n / 2) {
                log.info("Node:{} receives majority votes in term:{}", state.getMe(), state.getCurrentTerm());
                leaderInit();
                return;
            }
        }

        log.info("raft:{}'s current term:{}", state.getMe(), state.getCurrentTerm());
    }

    /**
     * 开启追加日志
     */
    public void doAppendEntries() {
        for (int i = 0; i < state.getPeers().length; i++) {
            if (i == state.getMe()) {
                continue;
            }

            int server = i;
            if (getFrontLog().getIndex() > state.getNextIndex()[i] - 1) {
                // 恢复快照线程
                new Thread(() -> doInstallSnapshot(server)).start();
            } else {
                // 日志复制线程
                new Thread(() -> appendTo(server)).start();
            }
        }
    }

    /**
     * 使用日志追加的方式复制
     *
     * @param server
     */
    public void appendTo(int server) {
        log.info("raft:{} sends entries to {}", state.getMe(), server);
        state.getLock().lock();
        if (state.getState() != RaftConstant.LEADER) {
            log.error("leader's state changed");
            state.getLock().unlock();
            return;
        }

        AppendEntriesRequest request = new AppendEntriesRequest();
        request.setTerm(state.getCurrentTerm());
        request.setLeaderId(state.getMe());
        request.setLeaderCommit(state.getCommitIndex());

        // 获取相应 raft 下一个日志对应的索引
        // 第一次获取的是 leader 最新的日志的索引
        int prevLogIndex = state.getNextIndex()[server] - 1;

        // 将日志索引转换成数组对应的索引
        int index = transfer(prevLogIndex);
        if (index == -1) {
            state.getLock().unlock();
            return;
        }

        Entry[] logs = state.getLogs();
        // 设置的是日志索引不是数组索引
        request.setPreLogIndex(logs[index].getIndex());
        request.setPreLogTerm(logs[index].getTerm());

        // 复制检查点之后的所有日志，用于 follower 的日志追赶
        // 因为 nextIndex 是乐观的，所以第一次发送请求的 entries 会为空，这时起着心跳的作用，如果不需要复制则不用修改 nextIndex 的值
        Entry[] entries = ArrayUtil.sub(logs, index + 1, logs.length);
        request.setEntries(entries);
        state.getLock().unlock();

        AppendEntriesResponse response = sendAppendEntries(server, request);
        if (response == null) {
            return;
        }
        System.out.println("raft:" + server + response);

        state.getLock().lock();
        // 检查 leader 状态
        if (response.getTerm() > state.getCurrentTerm()) {
            log.info("raft:{}'s term is larger than raft:{}'s", server, state.getMe());
            state.setCurrentTerm(response.getTerm());
            state.setVotedFor(-1);
            persist();
            trunTo(RaftConstant.FOLLOWER);
            state.getLock().unlock();
            return;
        }

        // 1.follower 复制成功则尝试提交
        if (response.isSucceeded()) {
            // 恢复 raft 对应的 nextIndex 的值
            state.getNextIndex()[server] = request.getPreLogIndex() + entries.length + 1;
            // 初次接收到对方的确认之后，更新悲观的 matchIndex
            state.getMatchIndex()[server] = request.getPreLogIndex() + entries.length;
            // 每有一次成功复制都去检查能否开始提交
            toCommit();
            log.info("nextIndex:{}, matchIndex:{}", state.getNextIndex(), state.getMatchIndex());
            state.getLock().unlock();
            return;
        }

        // 2.不成功则继续倒推 nextIndex，用于下一次发送复制请求
        log.info("start finding next index");
        for (int j = state.getNextIndex()[server] - 1; j >= 1; j--) {
            Entry entry = getEntry(j);
            if (entry == null || entry.getTerm() > response.getConflictTerm()) {
                continue;
            }

            if (entry.getTerm() == response.getConflictTerm()) {
                state.getNextIndex()[server] = j + 1;
                log.info("sets raft:{}'s nextIndex {}", server, state.getNextIndex()[server]);
                break;
            }
            if (entry.getTerm() < response.getConflictTerm()) {
                break;
            }
        }

        if (state.getNextIndex()[server] < 1) {
            state.getNextIndex()[server] = 1;
        }
        state.getLock().unlock();
    }

    /**
     * 复制机线程
     *
     * @param index
     */
    public void replicator(int index) {

    }

    /**
     * 通过日期类和循环实现的伪计时器
     * 处于 follower 状态并计时结束发起选举
     * 处于 leader 状态并计时结束发起心跳
     * 处于 candidate 状态(只会在一轮选举没选出 leader 的情况下触发)则重复选举
     * 每一次选举完都会重置选举时间,每次一次心跳都会重置对方的选举时间,每一次投完票都会重置自己的选举时间
     */
    public void ticker() {
        while (!killed()) {
            state.getLock().lock();
            switch (state.getState()) {
                case RaftConstant.FOLLOWER:
                    if (electionTimeout()) {
                        trunTo(RaftConstant.CANDIDATE);
                        log.info("raft:{} starts an election", state.getMe());
                        doElection();
                        resetElectionTimer();
                    }
                    break;
                case RaftConstant.LEADER:
                    if (heartbeatTimeout()) {
                        doAppendEntries();
                        resetHeartbeatTimer();
                    }
                    break;
                case RaftConstant.CANDIDATE:
                    if (electionTimeout()) {
                        trunTo(RaftConstant.CANDIDATE);
                        log.info("raft:{} starts a re-election", state.getMe());
                        doElection();
                        resetElectionTimer();
                    }
                    break;
                default:
                    break;
            }
            state.getLock().unlock();
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 多线程根据 killed 判断循环执行
     * 常态睡眠，当 raft 确认提交时会唤醒该线程
     * leader：检查到超过过半数 follower 复制成功则提交，更新 leaderCommit
     * follower：在 request 中检查到 leaderCommit 更新则提交
     */
    public void applier() {
        while (!killed()) {
            state.getLock().lock();
            Entry[] entries = null;
            try {
                while (state.getLastApplied() >= state.getCommitIndex()) {
                    state.getApplyCond().await();
                }

                int lastApplied = transfer(state.getLastApplied());
                int commitIndex = transfer(state.getCommitIndex());
                // 把新应用提交的日志内容通过通道告知 application
                entries = ArrayUtil.sub(state.getLogs(), lastApplied, commitIndex + 1);

                // 注意这里不能在提交日志给 application 之后再更新 lastApplied
                // 因为提交过程是不上锁的，在这个过程中 commitIndex 的值可能发生改变，导致 application 丢失一部分日志
                log.info("raft:{} applied index:{} from lastApplied:{}", state.getMe(), state.getCommitIndex(), state.getLastApplied());
                if (state.getLastApplied() < state.getCommitIndex()) {
                    state.setLastApplied(state.getCommitIndex());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                state.getLock().unlock();
            }

            // 循环遍历日志一条条发送
            Channel<ApplyMsg> channel = state.getChannel();
            log.info("raft:{} entries:{}", state.getMe(), entries);
            for (Entry entry : entries) {
                ApplyMsg applyMsg = new ApplyMsg();
                applyMsg.setCommandValid(true);
                applyMsg.setCommand(entry.getCommand());
                applyMsg.setCommandIndex(entry.getIndex());
                applyMsg.setCommandTerm(entry.getTerm());
                channel.writeOne(applyMsg);
                log.info("raft:{} applied {} to application", state.getMe(), entry);
            }
        }
    }

    /**
     * 发送投票请求
     *
     * @param server
     * @param request
     * @return
     */
    public RequestVoteResponse sendRequestVote(int server, RequestVoteRequest request) {
        RaftService[] peers = state.getPeers();
        RequestVoteResponse response;
        try {
            response = peers[server].requestVote(request);
        } catch (Exception e) {
            return null;
        }
        return response;
    }

    /**
     * 发送追加条目请求
     *
     * @param server
     * @param request
     * @return
     */
    public AppendEntriesResponse sendAppendEntries(int server, AppendEntriesRequest request) {
        RaftService[] peers = state.getPeers();
        AppendEntriesResponse response;
        try {
            response = peers[server].requestAppendEntries(request);
        } catch (Exception e) {
            log.warn(e.getMessage());
            return null;
        }
        return response;
    }

    /**
     * 发送心跳
     */
    public void heartbeat() {
        RaftService[] peers = state.getPeers();
        for (int i = 0; i < peers.length; i++) {
            if (i == state.getMe()) {
                continue;
            }
            try {
                final int server = i;
                CompletableFuture.runAsync(() -> {
                    peers[server].requestHeartbeat();
                }).get();
            } catch (Exception e) {
            }
        }
    }

    /**
     * 获取日志的最后一个条目
     *
     * @return
     */
    public Entry getLastLog() {
        Entry[] logs = state.getLogs();
        int n = logs.length;
        return logs[n - 1];
    }

    /**
     * 获取日志的第一个条目
     *
     * @return
     */
    public Entry getFrontLog() {
        Entry[] logs = state.getLogs();
        return logs[0];
    }

    /**
     * 将日志索引转换成数组对应的索引
     *
     * @param index
     * @return
     */
    public int transfer(int index) {
        int begin = getFrontLog().getIndex();
        int end = getLastLog().getIndex();
        if (index < begin || index > end) {
            log.warn("log index out of range");
            return -1;
        }
        return index - begin;
    }

    /**
     * 根据日志索引获取对应的日志
     *
     * @param index
     * @return
     */
    public Entry getEntry(int index) {
        int transfer = transfer(index);
        if (transfer == -1) {
            return null;
        }
        return state.getLogs()[transfer];
    }

    /**
     * 判断当前 leader 发来的追加条目内容是否和自身不一样
     * 是则开始复制
     *
     * @param request
     * @return
     */
    public boolean isConflict(AppendEntriesRequest request) {
        int baseIndex = request.getPreLogIndex() + 1;
        Entry[] entries = request.getEntries();
        for (int i = 0; i < entries.length; i++) {
            Entry entry = getEntry(baseIndex + i);
            if (entry == null || entry.getTerm() != entries[i].getTerm()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 判断日志是否最新
     *
     * @param lastLogIndex
     * @param lastLogTerm
     * @return
     */
    public boolean isLogUpToDate(int lastLogIndex, int lastLogTerm) {
        Entry lastLog = getLastLog();
        if (lastLog.getTerm() == lastLogTerm) {
            return lastLogIndex >= lastLog.getIndex();
        }
        return lastLogTerm > lastLog.getTerm();
    }

    /**
     * 检查所有 raft 的 matchIndex，尝试提交(leader 调用)
     */
    public void toCommit() {
        if (state.getCommitIndex() >= getLastLog().getIndex()) {
            return;
        }

        for (int i = getLastLog().getIndex(); i > state.getCommitIndex(); i--) {
            Entry entry = getEntry(i);
            if (entry == null) {
                continue;
            }
            if (entry.getTerm() != state.getCurrentTerm()) {
                return;
            }

            int cnt = 1;
            int n = state.getPeers().length;
            // 遍历所有的 raft 对应的 matchIndex(对应论文，标志着其他 raft 已经复制到的日志位置)
            // 超过半数 raft 已经复制完成则通知提交
            // 从 leader 的最新日志到 leader 上一次集体成功提交的位置遍历找到最新的能提交的位置
            for (int j = 0; j < n; j++) {
                if (j != state.getMe() && state.getMatchIndex()[j] >= i) {
                    cnt++;
                }
                log.info("commit check; {} rafts commit to index {}", cnt, i);
                if (cnt > n / 2) {
                    state.setCommitIndex(i);
                    log.info("raft:{} commit to {}", state.getMe(), state.getCommitIndex());
                    state.getApplyCond().signal();
                    return;
                }
            }
        }

        log.info("raft:{} doesn't have half replicated from {} to {} now", state.getMe(), state.getCommitIndex(), getLastLog().getIndex());
    }

    /**
     * 判断自身是否为 leader
     *
     * @return
     */
    public boolean isLeader() {
        state.getLock().lock();
        int state1 = state.getState();
        state.getLock().unlock();
        return state1 == RaftConstant.LEADER;
    }

    /**
     * 初始化 leader 状态
     */
    public void leaderInit() {
        state.setState(RaftConstant.LEADER);
        int n = state.getPeers().length;
        state.setNextIndex(new int[n]);
        state.setMatchIndex(new int[n]);

        for (int i = 0; i < n; i++) {
            state.getNextIndex()[i] = getLastLog().getIndex() + 1;
            state.getMatchIndex()[i] = 0;
        }

        resetHeartbeatTimer();
    }

    /**
     * 切换 raft 状态
     *
     * @param turn
     */
    public void trunTo(int turn) {
        switch (turn) {
            case RaftConstant.LEADER:
                leaderInit();
                break;
            case RaftConstant.CANDIDATE:
                state.setVotedFor(state.getMe());
                state.setState(RaftConstant.CANDIDATE);
                state.setCurrentTerm(state.getCurrentTerm() + 1);
                persist();
                break;
            case RaftConstant.FOLLOWER:
                state.setState(RaftConstant.FOLLOWER);
        }
    }

    /**
     * 获取自身当前所处的 term
     *
     * @return
     */
    public int getTerm() {
        state.getLock().lock();
        int term = state.getCurrentTerm();
        state.getLock().unlock();
        return term;
    }

    /**
     * 生成一个 request 请求体
     *
     * @return
     */
    public RequestVoteRequest genRequest() {
        RequestVoteRequest request = new RequestVoteRequest();
        request.setTerm(state.getCurrentTerm());
        request.setCandidateId(state.getMe());
        Entry lastLog = getLastLog();
        request.setLastLogTerm(lastLog.getTerm());
        request.setLastLogIndex(lastLog.getIndex());
        return request;
    }

    /**
     * 持久化恢复
     *
     * @param
     */
    public void readPersist() {
        state.getPersister().readPersist(state);
    }

    /**
     * 持久化,在持有锁的时候调用，所以不用加锁
     * 当 log，votedFor，term 发生变化时调用
     */
    public void persist() {
        state.getPersister().persist(state);
    }

    /**
     * 安装快照
     *
     * @param lastIncludedTerm
     * @param lastIncludedIndex
     * @param snapshot
     * @return
     */
    public boolean condInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, byte[] snapshot) {
        return true;
    }

    /**
     * 生成快照
     *
     * @param index
     * @param snapshot
     */
    public void snapshot(int index, byte[] snapshot) {

    }

    /**
     * 在日志中新增一条日志，由 application(外部) 调用
     * 只能给 leader 新增日志，然后由 leader 复制给其他机器(对外单机)
     *
     * @return 返回追加日志后最新的日志索引
     */
    public int startCmd(Object cmd) {
        state.getLock().lock();
        if (state.getState() != RaftConstant.LEADER) {
            state.getLock().unlock();
            return -1;
        }

        int index = getLastLog().getIndex() + 1;
        state.setLogs(ArrayUtil.append(state.getLogs(), new Entry(index, state.getCurrentTerm(), cmd)));
        persist();
        state.getLock().unlock();

        log.info("raft:{}: appends a cmd:{}, lastLogIndex:{}", state.getMe(), cmd, getLastLog().getIndex());
        if (!killed()) {
            doAppendEntries();
        }
        return index;
    }

    /**
     * kill a raft
     */
    public void kill() {
        state.getDead().set(1);
    }

    /**
     * restart a raft
     */
    public void restart() {
        state.getDead().set(0);
        trunTo(RaftConstant.FOLLOWER);
        resetElectionTimer();
        new Thread(this::ticker).start();
        new Thread(this::applier).start();
    }

    public boolean killed() {
        return state.getDead().get() == 1;
    }

    /**
     * 使用安装快照恢复的方式复制日志
     *
     * @param server
     */
    public void doInstallSnapshot(int server) {

    }

    /**
     * 判断选举计时是否结束
     *
     * @return
     */
    public boolean electionTimeout() {
        return state.getElectionTimer().isBefore(LocalDateTime.now());
    }

    /**
     * 判断心跳计时是否结束
     *
     * @return
     */
    public boolean heartbeatTimeout() {
        return state.getHeartbeatTimer().isBefore(LocalDateTime.now());
    }

    /**
     * 重置选举计时器
     */
    public void resetElectionTimer() {
        int time = random.nextInt(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN + 1) + ELECTION_TIMEOUT_MIN;
        state.setElectionTimer(LocalDateTime.now().plus(Duration.ofMillis(time)));
    }

    /**
     * 重置心跳计时器
     */
    public void resetHeartbeatTimer() {
        state.setHeartbeatTimer(LocalDateTime.now().plus(Duration.ofMillis(HEARTBEAT_INTERVAL)));
    }

}
