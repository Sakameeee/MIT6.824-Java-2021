package com.sakame.config;

import com.sakame.KVServer;
import com.sakame.Persister;
import com.sakame.model.ServiceMetaInfo;
import com.sakame.server.VertxHttpServer;
import com.sakame.service.KVServerService;
import com.sakame.service.RaftService;
import lombok.Data;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * KVServer 配置
 *
 * @author sakame
 * @version 1.0
 */
@Data
public class KVRaftConfig {

    /**
     * 单例
     */
    private static KVRaftConfig config = new KVRaftConfig();

    /**
     * 锁
     */
    private Lock lock = new ReentrantLock();

    /**
     * KVServer 实例数组
     */
    private KVServer[] kvServers;

    /**
     * KVServer 服务调用数组
     */
    private KVServerService[] kvServerServices;

    /**
     * raft service 服务调用数组
     */
    private RaftService[] peers;

    /**
     * raft httpserver
     */
    private VertxHttpServer[] raftServers;

    /**
     * kv raft httpserver
     */
    private VertxHttpServer[] kvRaftServers;

    /**
     * raft 服务信息
     */
    private ServiceMetaInfo[] raftServiceInfos;

    /**
     * KVServer 服务信息
     */
    private ServiceMetaInfo[] kvRaftServiceInfos;

    /**
     * server 数量
     */
    private int count;

    /**
     * raft 的持久化对象数组
     */
    private Persister[] saved;

    /**
     * 记录生成的 client 个数
     */
    private int nextClientId;

    /**
     * snapshot 模式下裁切允许的最大容量
     */
    private int maxRaftState;

    public static KVRaftConfig getConfig() {
        return config;
    }

}
