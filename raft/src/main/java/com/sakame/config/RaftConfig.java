package com.sakame.config;

import com.sakame.Persister;
import com.sakame.Raft;
import com.sakame.model.ServiceMetaInfo;
import com.sakame.server.VertxHttpServer;
import com.sakame.service.RaftService;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author sakame
 * @version 1.0
 */
@Data
public class RaftConfig {

    private static RaftConfig raftConfig = new RaftConfig();
    /**
     * 锁
     */
    private ReentrantLock lock = new ReentrantLock();
    /**
     * raft 个数
     */
    private int raftCount;
    /**
     * raft 实例数组
     */
    private Raft[] rafts;
    /**
     * 是否连接
     */
    private boolean[] connected;
    /**
     * 持久化实例
     */
    private Persister[] saved;
    /**
     * httpserver 实例
     */
    private VertxHttpServer[] servers;
    /**
     * raft service 服务调用
     */
    private RaftService[] peers;
    /**
     * raft service
     */
    private ServiceMetaInfo[] services;
    /**
     * 所有 raft 的日志条目
     */
    private Map<Integer, Object>[] logs;
    /**
     * 应用启动时间
     */
    private LocalDateTime startTime;
    /**
     * 测试开始时间
     */
    private LocalDateTime testTime;
    /**
     * raft 中日志的最大索引
     */
    private int maxIndex;

    public static RaftConfig getInstance() {
        return raftConfig;
    }

}
