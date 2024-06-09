package com.sakame;

import com.sakame.common.CommandContext;
import com.sakame.common.CommandType;
import com.sakame.constant.Message;
import com.sakame.constant.TimeConstant;
import com.sakame.model.Channel;
import com.sakame.model.Command;
import com.sakame.model.IndexAndTerm;
import com.sakame.model.ServerState;
import com.sakame.model.dto.ApplyMsg;
import com.sakame.model.dto.CommandRequest;
import com.sakame.model.dto.CommandResponse;
import com.sakame.server.VertxHttpServer;
import com.sakame.service.KVServerService;
import com.sakame.service.RaftService;
import lombok.extern.slf4j.Slf4j;

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

        new Thread(this::applier).start();
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
     * 代替了 RaftApplication 的 applier，接收来自 leader 提交的日志
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
