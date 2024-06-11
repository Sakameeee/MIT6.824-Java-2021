package com.sakame;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.ArrayUtil;
import com.sakame.common.CommandContext;
import com.sakame.common.CommandType;
import com.sakame.constant.Message;
import com.sakame.constant.TimeConstant;
import com.sakame.model.*;
import com.sakame.model.dto.ApplyMsg;
import com.sakame.model.dto.CommandRequest;
import com.sakame.model.dto.CommandResponse;
import com.sakame.serializer.Serializer;
import com.sakame.serializer.SerializerFactory;
import com.sakame.serializer.SerializerKeys;
import com.sakame.server.VertxHttpServer;
import com.sakame.service.KVServerService;
import com.sakame.service.RaftService;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;

/**
 * @author sakame
 * @version 1.0
 */
@Slf4j
public class KVServer implements KVServerService {

    private ServerState state = new ServerState();

    private static final int SNAPSHOT_LOG_GAP = 3;

    private static final double THRESHOLD = 0.8;

    public VertxHttpServer init(int me, RaftService[] peers, Persister persister, int maxRaftState) {
        state.setMe(me);
        state.setMaxRaftState(maxRaftState);
        state.setChannel(new Channel<>(5));
        Raft raft = new Raft();
        state.setRaft(raft);
        raft.init(peers, me, persister, state.getChannel());
        state.setKvMap(new KV());
        state.setCmdResponseChannels(new HashMap<>());
        state.setLastCmdContext(new HashMap<>());
        state.setLastApplied(0);
        state.setLastSnapshot(0);

        // 每次初始化都从持久化对象中恢复数据，只读取 snapshot 而非 raftState
        installSnapshot(persister.readSnapshot());

        new Thread(this::applier).start();
        new Thread(this::snapshoter).start();
        return new VertxHttpServer(raft);
    }

    @Override
    public CommandResponse requestCommand(CommandRequest request) {
        state.getLock().lock();
        log.info("{} received command request {}", state.getMe(), request);

        Command command = request.getCommand();
        CommandType type = command.getType();
        int clientId = command.getClientId();
        int seqId = command.getSeqId();
        // 检查操作是否可行
        if (!type.equals(CommandType.GET) && isDuplicated(clientId, seqId)) {
            CommandContext context = state.getLastCmdContext().get(clientId);
            state.getLock().unlock();
            return context.getResponse();
        }
        state.getLock().unlock();

        CommandResponse response = new CommandResponse();
        Command cmd = new Command();
        cmd.setType(type);
        cmd.setKey(command.getKey());
        cmd.setValue(command.getValue());
        cmd.setClientId(clientId);
        cmd.setSeqId(seqId);
        // 将命令追加到 raft 之中，随后开启通道等待来自 applier 的消息
        Raft raft = state.getRaft();
        int index = raft.startCmd(cmd);
        int term = raft.getTerm();
        // 检查状态，是否为 leader
        if (index == -1) {
            response.setValue("");
            response.setSuccess(false);
            response.setErr(Message.WRONG_LEADER);
            return response;
        }

        state.getLock().lock();
        // 新建一个通道，供 applier 写入，该函数只创建通道和读取通道的信息
        IndexAndTerm it = new IndexAndTerm(index, term);
        Channel<CommandResponse> channel = new Channel<>(1);
        state.getCmdResponseChannels().put(it, channel);
        state.getLock().unlock();

        // 在一定的时间段里反复读取通道里的消息
        LocalDateTime time = LocalDateTime.now().plus(TimeConstant.CMD_TIMEOUT, ChronoUnit.MILLIS);
        while (LocalDateTime.now().isBefore(time)) {
            state.getLock().lock();
            CommandResponse res = null;
            if ((res = channel.read()) != null) {
                log.info("{} had applied, {}", state.getMe(), res);
                // 销毁通道
                state.getCmdResponseChannels().remove(it);
                state.getLock().unlock();
                return res;
            }
            state.getLock().unlock();
            try {
                Thread.sleep(TimeConstant.GAP_TIME);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        response.setValue("");
        response.setSuccess(false);
        response.setErr(Message.TIMEOUT);
        state.getCmdResponseChannels().remove(it);
        return response;
    }

    /**
     * 代替了 RaftApplication 的 applierSnap，接收来自 leader 提交的日志
     * handler 向 raft 中追加命令，raft 只负责记录命令序列
     * 在保证一致性的情况下 raft 会向 applier 提交命令，实际调用 kv 存储并执行命令是在 applier 中完成的
     */
    public void applier() {
        Channel<ApplyMsg> channel = state.getChannel();
        while (!killed()) {
            ApplyMsg applyMsg = null;
            if ((applyMsg = channel.read()) != null) {
                if (applyMsg.isCommandValid()) {
                    state.getLock().lock();

                    // 检查消息是否过时
                    if (applyMsg.getCommandIndex() < state.getLastApplied()) {
                        log.info("{} was too old", applyMsg);
                        state.getLock().unlock();
                        continue;
                    }
                    state.setLastApplied(applyMsg.getCommandIndex());

                    // 判断命令类型并记录
                    Command command = (Command) applyMsg.getCommand();
                    CommandResponse response = new CommandResponse();
                    int seqId = command.getSeqId();
                    int clientId = command.getClientId();
                    if (command.getType() != CommandType.GET && isDuplicated(clientId, seqId)) {
                        // 如果存在对应的命令记录则获取 response
                        CommandContext context = state.getLastCmdContext().get(clientId);
                        response = context.getResponse();
                    } else {
                        // 如果不存在记录则调用 kv 的 opt 获取并初始化 response
                        String value = state.getKvMap().opt(command);
                        response.setValue(value);
                        response.setSuccess(true);
                        // 更新命令上下文
                        CommandContext context = new CommandContext(seqId, response);
                        state.getLastCmdContext().put(clientId, context);
                    }

                    // 检查状态
                    if (!state.getRaft().isLeader() || applyMsg.getCommandTerm() != state.getRaft().getTerm()) {
                        state.getLock().unlock();
                        continue;
                    }

                    // 通过通道把 response 返回给 handler
                    Channel<CommandResponse> responseChannel = state.getCmdResponseChannels().get(new IndexAndTerm(applyMsg.getCommandIndex(), applyMsg.getCommandTerm()));
                    if (responseChannel != null && (!responseChannel.write(response))) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    state.getLock().unlock();
                } else if (applyMsg.isSnapShotValid()) {
                    state.getLock().lock();
                    // follower 会在每次生成快照后进入这个分支一次，raft 和 kv server 同时完成快照的安装
                    if (state.getRaft().condInstallSnapshot(applyMsg.getSnapShotTerm(), applyMsg.getSnapShotIndex(), applyMsg.getSnapShot())) {
                        installSnapshot(applyMsg.getSnapShot());
                        state.setLastApplied(applyMsg.getSnapShotIndex());
                    }
                    state.getLock().unlock();
                }
            } else {
                try {
                    Thread.sleep(TimeConstant.GAP_TIME);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * kv server 初始化时开启该线程，检查是否需要裁切日志生成快照
     */
    public void snapshoter() {
        while (!killed()) {
            state.getLock().lock();
            if (isNeedSnapshot()) {
                doSnapshot(state.getLastApplied());
                state.setLastSnapshot(state.getLastApplied());
            }
            state.getLock().unlock();
            try {
                Thread.sleep(TimeConstant.SNAPSHOT_GAP_TIME);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 生成快照，由 snapshoter 调用，实质是生成数据后调用 raft 的 snapshot
     *
     * @param commandIndex
     */
    public void doSnapshot(int commandIndex) {
        log.info("kv server {} do snapshot", state.getMe());
        final Serializer serializer = SerializerFactory.getInstance(SerializerKeys.KRYO);
        ServerStatePersist statePersist = new ServerStatePersist();
        BeanUtil.copyProperties(state, statePersist);
        byte[] snapshot;
        try {
            snapshot = serializer.serialize(statePersist);
            if (ArrayUtil.isEmpty(snapshot)) {
                throw new RuntimeException("snapshot failure");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        state.getRaft().snapshot(commandIndex, snapshot);
    }

    /**
     * 安装快照，和 raft 中的 condInstallSnapshot 差不多
     *
     * @param snapshot
     */
    public void installSnapshot(byte[] snapshot) {
        if (ArrayUtil.isEmpty(snapshot)) {
            return;
        }

        log.info("kv server {} install snapshot", state.getMe());
        final Serializer serializer = SerializerFactory.getInstance(SerializerKeys.KRYO);
        try {
            ServerStatePersist deserialize = serializer.deserialize(snapshot, ServerStatePersist.class);
            BeanUtil.copyProperties(deserialize, state);
        } catch (IOException e) {
            throw new RuntimeException("install snapshot failure");
        }
    }

    /**
     * 检查是否需要生成快照（一般是 leader 调用）
     * 1.是否开启 snapshot
     * 2.raft state（带有日志会逐渐变大）是否超过预期大小
     * 3.lastApplied 和 lastSnapshot 的间隔是否大于预期值
     *
     * @return
     */
    public boolean isNeedSnapshot() {
        if (state.getMaxRaftState() != -1
                && state.getRaft().getRaftPersistSize() > THRESHOLD * state.getMaxRaftState()
                && state.getLastApplied() > state.getLastSnapshot() + SNAPSHOT_LOG_GAP) {
            return true;
        }
        return false;
    }

    /**
     * 在完成 put 或 append 操作前要通过这个检查
     * 即要求对应的命令为空或者追加位置大于最后一个位置
     *
     * @param clientId
     * @param seqId
     * @return
     */
    public boolean isDuplicated(int clientId, int seqId) {
        CommandContext context = state.getLastCmdContext().get(clientId);
        return context != null && context.getSeqId() >= seqId;
    }

    public boolean isLeader() {
        return state.getRaft().isLeader();
    }

    public void kill() {
        state.getDead().set(1);
        state.getRaft().kill();
    }

    public boolean killed() {
        return state.getDead().get() == 1;
    }

}
