package com.sakame.model;

import com.sakame.KV;
import com.sakame.Raft;
import com.sakame.common.CommandContext;
import com.sakame.model.dto.ApplyMsg;
import com.sakame.model.dto.CommandResponse;
import lombok.Data;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * KVServer 配置
 *
 * @author sakame
 * @version 1.0
 */
@Data
public class ServerState {

    /**
     * 锁
     */
    private Lock lock = new ReentrantLock();

    /**
     * 自身标识
     */
    private int me;

    /**
     * 该 server 所使用的 raft
     */
    private Raft raft;

    /**
     * 用于 raft 与上层的 kv 通信
     */
    private Channel<ApplyMsg> channel;

    /**
     * 状态标识，原子化
     */
    private AtomicInteger dead = new AtomicInteger(0);

    /**
     * 所能存储的最大 raft 状态，snapshot 模式下使用
     */
    private int maxRaftState;

    /**
     * 该 server 所使用的 kv 存储对象
     */
    private KV kvMap;

    /**
     * 用于某个时期临时的 response 通讯，applier 告知 handler 从 raft 获取的命令生成的 response，handler 会在返回 response 之前销毁通道
     */
    private Map<IndexAndTerm, Channel<CommandResponse>> cmdResponseChannels;

    /**
     * clientId 对应的最后一次提交命令的上下文信息
     */
    private Map<Integer, CommandContext> lastCmdContext;

    /**
     * 最后一次 raft 向 kv 提交条目的索引
     */
    private int lastApplied;

    /**
     * 最后一次生成快照的位置
     */
    private int lastSnapshot;

}
