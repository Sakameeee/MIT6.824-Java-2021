package com.sakame.model;

import com.sakame.Persister;
import com.sakame.constant.RaftConstant;
import com.sakame.model.dto.ApplyMsg;
import com.sakame.service.RaftService;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * rafe state
 *
 * @author sakame
 * @version 1.0
 */
@Data
public class RaftState {

    /**
     * 锁
     */
    private ReentrantLock lock = new ReentrantLock();

    /**
     * 持久化恢复
     */
    private Persister persister;

    /**
     * raft service 服务调用
     */
    private RaftService[] peers;

    /**
     * 自身标识
     */
    private int me;

    /**
     * 是否宕机
     */
    private AtomicInteger dead = new AtomicInteger(0);

    /**
     * 本机当前所处的 term（持久化）
     */
    private int currentTerm = 0;

    /**
     * 本机已投票信息（持久化）
     */
    private int votedFor = -1;

    /**
     * 本机日志条目（持久化）
     */
    private Entry[] logs;

    /**
     * 用于在提交新条目之后唤醒 applyLog 线程
     */
    private Condition applyCond = lock.newCondition();

    /**
     * 用于和 application 通信
     */
    private Channel<ApplyMsg> channel;

    /**
     * 用于唤醒 replicator 线程开始复制日志
     */
    private Condition[] replicatorCond;

    /**
     * 本机当前状态
     */
    private int state = RaftConstant.FOLLOWER;

    /**
     * 最近提交的日志条目的索引位置
     */
    private int commitIndex = 0;

    /**
     * 最后一次应用提交的日志条目的索引位置
     */
    private int lastApplied = 0;

    /**
     * 下一要写入日志的索引位置（leader 维护）
     */
    private int[] nextIndex;

    /**
     * 日志内容匹配的最近索引位置（leader 维护）
     */
    private int[] matchIndex;

    /**
     * 选举计时器
     */
    private LocalDateTime electionTimer;

    /**
     * 心跳机制计时器
     */
    private LocalDateTime heartbeatTimer;

}
