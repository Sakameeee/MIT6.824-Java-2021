package com.sakame;

import com.sakame.config.KVRaftConfig;
import com.sakame.config.RegistryConfig;
import com.sakame.config.RpcConfig;
import com.sakame.constant.RpcConstant;
import com.sakame.model.ServiceMetaInfo;
import com.sakame.proxy.ServiceProxyFactory;
import com.sakame.registry.Registry;
import com.sakame.registry.RegistryFactory;
import com.sakame.server.VertxHttpServer;
import com.sakame.service.KVServerService;
import com.sakame.service.RaftService;
import com.sakame.utils.RpcUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.stream.IntStream;

/**
 * @author sakame
 * @version 1.0
 */
@Slf4j
public class KVRaftApplication {

    private final KVRaftConfig config = KVRaftConfig.getConfig();

    private static final int RAFT_PORT = 22222;

    private static final int KV_SERVER_PORT = 8888;

    private Registry registry;

    public void init(int n, int maxRaftState) {
        config.setCount(n);
        config.setMaxRaftState(maxRaftState);
        config.setSaved(new Persister[n]);
        config.setNextClientId(n + 1000);
        config.setKvServerServices(new KVServerService[n]);
        config.setRaftServiceInfos(new ServiceMetaInfo[n]);
        config.setKvServers(new KVServer[n]);
        config.setKvRaftServiceInfos(new ServiceMetaInfo[n]);
        config.setKvRaftServers(new VertxHttpServer[n]);
        config.setRaftServers(new VertxHttpServer[n]);
        config.setPeers(new RaftService[n]);
        config.setKvServerServices(new KVServerService[n]);

        RpcConfig rpcConfig = RpcConfig.getRpcConfig();
        RegistryConfig registryConfig = rpcConfig.getRegistryConfig();
        registry = RegistryFactory.getInstance(registryConfig.getRegistry());

        // 1.注册 Raft 和 KVServer 服务
        for (int i = 0; i < n; i++) {
            ServiceMetaInfo serviceMetaInfo = new ServiceMetaInfo();
            serviceMetaInfo.setServiceName(RaftService.class.getName());
            serviceMetaInfo.setServiceVersion(RpcConstant.DEFAULT_SERVICE_VERSION);
            serviceMetaInfo.setServiceHost(rpcConfig.getServerHost());
            serviceMetaInfo.setServicePort(RAFT_PORT + i);
            config.getRaftServiceInfos()[i] = serviceMetaInfo;
            ServiceMetaInfo serviceMetaInfo1 = new ServiceMetaInfo();
            serviceMetaInfo1.setServiceName(KVServerService.class.getName());
            serviceMetaInfo1.setServiceVersion(RpcConstant.DEFAULT_SERVICE_VERSION);
            serviceMetaInfo1.setServiceHost(rpcConfig.getServerHost());
            serviceMetaInfo1.setServicePort(KV_SERVER_PORT + i);
            config.getKvRaftServiceInfos()[i] = serviceMetaInfo1;
            try {
                registry.register(serviceMetaInfo);
                registry.register(serviceMetaInfo1);
            } catch (Exception e) {
                throw new RuntimeException("fail to start raft", e);
            }
        }

        // 2.获取服务调用
        for (int i = 0; i < n; i++) {
            config.getPeers()[i] = ServiceProxyFactory.getProxy(RaftService.class, i);
            config.getKvServerServices()[i] = ServiceProxyFactory.getProxy(KVServerService.class, i);
        }

        // 3.启动服务器
        for (int i = 0; i < n; i++) {
            startServer(i);
        }

        // 4.连接一切
        connectAll();
    }

    /**
     * 将 kvServers 分为两组，并将 leader 置于较小的一组当中
     *
     * @return p1 为 majority，p2 为 minority
     */
    public int[][] makePartition() {
        int n = config.getCount();
        int leader = getLeader();
        int[][] ret = new int[2][];
        ret[0] = new int[n / 2 + 1];
        ret[1] = new int[n / 2];
        int j = 0;
        for (int i = 0; i < n; i++) {
            if (i != leader) {
                if (j < ret[0].length) {
                    ret[0][j] = i;
                } else {
                    ret[1][j - ret[0].length] = i;
                }
                j++;
            }
        }
        ret[1][ret[1].length - 1] = leader;
        return ret;
    }

    /**
     * 根据 p1 和 p2 两个数组制造分区
     *
     * @param p1 第一个分区
     * @param p2 第二个分区
     */
    public void partition(int[] p1, int[] p2) {
        config.getLock().lock();
        for (int i = 0; i < p1.length; i++) {
            disconnectUnlocked(p1[i], p2);
            connectUnlocked(p1[i], p1);
        }
        for (int i = 0; i < p2.length; i++) {
            disconnectUnlocked(p2[i], p1);
            connectUnlocked(p2[i], p2);
        }
        config.getLock().unlock();
    }

    /**
     * 启动一个 kvServer 服务端
     * 包含 raft httpserver 和 kv httpserver
     *
     * @param server
     */
    public void startServer(int server) {
        config.getLock().lock();
        if (config.getSaved()[server] != null) {
            config.getSaved()[server] = config.getSaved()[server].clone();
        } else {
            config.getSaved()[server] = new Persister();
        }
        config.getLock().unlock();

        KVServer kvServer = new KVServer();
        config.getRaftServers()[server] = kvServer.init(server, config.getPeers(), config.getSaved()[server], config.getMaxRaftState());
        config.getKvRaftServers()[server] = new VertxHttpServer(kvServer);
        config.getKvServers()[server] = kvServer;
        config.getKvRaftServers()[server].doStart(config.getKvRaftServiceInfos()[server].getServicePort());
        config.getRaftServers()[server].doStart(config.getRaftServiceInfos()[server].getServicePort());

        connectUnlocked(server, all());
    }

    /**
     * 与 startServer 相对，用于完整关闭一个 kvServer
     *
     * @param server
     */
    public void shutdownServer(int server) {
        config.getLock().lock();
        disconnectUnlocked(server, all());
        config.getRaftServers()[server].doShutdown();
        config.getKvRaftServers()[server].doShutdown();
        log.info("shutdown kv server {}", server);


        Persister persister = config.getSaved()[server];
        if (persister != null) {
            config.getSaved()[server] = persister.clone();
        }

        KVServer kvServer = config.getKvServers()[server];
        if (kvServer != null) {
            config.getLock().unlock();
            kvServer.kill();
            config.getLock().lock();
            config.getKvServers()[server] = null;
        }
        config.getLock().unlock();
    }

    /**
     * 以不上锁的形式启用某个 raft 和其他集群之间互相的调用权限
     *
     * @param i
     * @param to
     */
    public void connectUnlocked(int i, int[] to) {
        for (int j = 0; j < to.length; j++) {
            RpcUtils.enable(RaftService.class, i, to[j]);
            RpcUtils.enable(RaftService.class, to[j], i);
        }
    }

    public void connect(int i, int[] to) {
        config.getLock().lock();
        connectUnlocked(i, to);
        config.getLock().unlock();
    }

    /**
     * 以不上锁的形式停用某个 raft 和其他集群之间互相的调用权限
     *
     * @param i
     * @param from
     */
    public void disconnectUnlocked(int i, int[] from) {
        for (int j = 0; j < from.length; j++) {
            RpcUtils.disable(RaftService.class, i, from[j]);
            RpcUtils.disable(RaftService.class, from[j], i);
        }
    }

    public void disconnect(int i, int[] from) {
        config.getLock().lock();
        disconnectUnlocked(i, from);
        config.getLock().unlock();
    }

    /**
     * 启用所有 raft 之间的调用权限
     */
    public void connectAll() {
        config.getLock().lock();
        for (int i = 0; i < config.getCount(); i++) {
            connectUnlocked(i, all());
        }
        config.getLock().unlock();
    }

    public int[] all() {
        return IntStream.range(0, config.getCount()).toArray();
    }

    /**
     * 构建一个 client
     *
     * @return
     */
    public Client makeClient() {
        config.getLock().lock();
        Client client = new Client(config.getKvServerServices());
        config.setNextClientId(config.getNextClientId() + 1);
        config.getLock().unlock();
        return client;
    }

    /**
     * 以不上锁的形式启用该 client 对 kvServer 分组的调用权限
     *
     * @param client
     * @param to
     */
    public void connectClientUnlocked(Client client, int[] to) {
        for (int x : to) {
            RpcUtils.enable(KVServerService.class, Integer.valueOf(client.getId()), x);
        }
    }

    public void connectClient(Client client, int[] to) {
        config.getLock().lock();
        connectClientUnlocked(client, to);
        config.getLock().unlock();
    }

    /**
     * 以不上锁的形式停用该 client 对 kvServer 分组的调用权限
     *
     * @param client
     * @param from
     */
    public void disconnectClientUnlocked(Client client, int[] from) {
        for (int x : from) {
            RpcUtils.disable(KVServerService.class, client.getId(), x);
        }
    }

    public void disconnectClient(Client client, int[] from) {
        config.getLock().lock();
        disconnectClientUnlocked(client, from);
        config.getLock().unlock();
    }

    public int logSize() {
        int logsize = 0;
        for (int i = 0; i < config.getCount(); i++) {
            int n = config.getSaved()[i].raftStateSize();
            if (n > logsize) {
                logsize = n;
            }
        }
        return logsize;
    }

    public int snapshotSize() {
        int snapshotSize = 0;
        for (int i = 0; i < config.getCount(); i++) {
            int n = config.getSaved()[i].snapshotSize();
            if (n > snapshotSize) {
                snapshotSize = n;
            }
        }
        return snapshotSize;
    }

    public int getNServers() {
        return config.getCount();
    }

    public int getLeader() {
        for (int i = 0; i < config.getCount(); i++) {
            if (config.getKvServers()[i].isLeader()) {
                return i;
            }
        }
        return -1;
    }

    public void cleanup() {
        int n = config.getCount();
        for (int i = 0; i < n; i++) {
            config.getKvServers()[i].kill();
            config.getRaftServers()[i].doShutdown();
            config.getKvRaftServers()[i].doShutdown();
            registry.unRegister(config.getKvRaftServiceInfos()[i]);
            registry.unRegister(config.getRaftServiceInfos()[i]);
        }
    }

}
