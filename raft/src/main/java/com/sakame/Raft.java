package com.sakame;

import com.sakame.constant.RaftConstant;
import com.sakame.model.Entry;
import com.sakame.model.RaftState;
import com.sakame.model.dto.ApplyMsg;
import com.sakame.model.dto.RequestVoteRequest;
import com.sakame.model.dto.RequestVoteResponse;
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

    private RaftState state;

    private final Random random = new Random();

    private static final int HEARTBEAT_INTERVAL = 50;

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        // 尝试获取锁,避免死锁,如果获取不到则说明对方也在发起选举
        boolean tried = state.getLock().tryLock();
        if (!tried) {
            return null;
        }

        RequestVoteResponse response = new RequestVoteResponse();
        log.info("raft:{} receive RequestVoteRequest:{}", state.getMe(), request);
        log.info("raft:{} current state:term:{}, votedFor:{}, lastLog:{}",state.getMe(), state.getCurrentTerm(), state.getVotedFor(), getLastLog());

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
            state.setState(RaftConstant.FOLLOWER);
        }

        if (state.getVotedFor() == -1 || state.getVotedFor() == request.getCandidateId()) {
//            if (!isLogUpToDate(request.getLastLogIndex(), request.getLastLogTerm())) {
//                response.setVoteGranted(false);
//                response.setTerm(state.getCurrentTerm());
//                persist();
//                state.getLock().unlock();
//                return response;
//            }

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
     * 判断日志是否最新
     *
     * @param lastLogIndex
     * @param lastLogTerm
     * @return
     */
    public boolean isLogUpToDate(int lastLogIndex, int lastLogTerm) {
        Entry lastLog = getLastLog();
        if (state.getCurrentTerm() == lastLogTerm) {
            return lastLogIndex >= lastLog.getIndex();
        }
        return lastLogTerm > lastLog.getTerm();
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
        state.setVotedFor(state.getMe());
        state.setState(RaftConstant.CANDIDATE);
        state.setCurrentTerm(state.getCurrentTerm() + 1);
        persist();
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
     * @param data
     */
    public void readPersist(byte[] data) {

    }

    /**
     * 持久化
     */
    public void persist() {

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

    public void start() {

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
        state.setState(RaftConstant.FOLLOWER);
        resetElectionTimer();
        new Thread(this::ticker).start();
    }

    public boolean killed() {
        return state.getDead().get() == 1;
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
                        state.setState(RaftConstant.FOLLOWER);
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

        state.setCurrentTerm(state.getCurrentTerm() - 1);
        log.info("raft:{}'s current term:{}", state.getMe(), state.getCurrentTerm());
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
            } catch (Exception e) {}
        }
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
                        log.info("raft:{} starts an election", state.getMe());
                        doElection();
                        resetElectionTimer();
                    }
                    break;
                case RaftConstant.LEADER:
                    if (heartbeatTimeout()) {
                        heartbeat();
                        resetHeartbeatTimer();
                    }
                    break;
                case RaftConstant.CANDIDATE:
                    if (electionTimeout()) {
                        log.info("raft:{} starts a re-election", state.getMe());
                        doElection();
                        resetElectionTimer();
                    }
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

    public void applier() {

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
        int time = random.nextInt(400 - 150 + 1) + 150;
        state.setElectionTimer(LocalDateTime.now().plus(Duration.ofMillis(time)));
    }

    /**
     * 重置心跳计时器
     */
    public void resetHeartbeatTimer() {
        state.setHeartbeatTimer(LocalDateTime.now().plus(Duration.ofMillis(HEARTBEAT_INTERVAL)));
    }

    /**
     * 初始化一个 raft，一个 raft 代表一个线程
     *
     * @param peers
     * @param me
     * @param persister
     * @param applyCh
     */
    public void init(RaftService[] peers, int me, Persister persister, ApplyMsg applyCh) {
        int n = peers.length;
        state = new RaftState();
        // 初始化状态
        state.setPeers(peers);
        state.setPersister(persister);
        state.setMe(me);
        state.setReplicatorCond(new Condition[n]);
        state.setLogs(new Entry[]{new Entry()});
        resetElectionTimer();
        resetHeartbeatTimer();
//        System.out.println(state.getMe() + state.getElectionTimer().);
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
        new Thread(this::applier);
    }

}
