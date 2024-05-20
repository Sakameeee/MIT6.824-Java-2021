package com.sakame;

import com.sakame.config.RaftConfig;
import com.sakame.config.RegistryConfig;
import com.sakame.config.RpcConfig;
import com.sakame.constant.RpcConstant;
import com.sakame.model.ServiceMetaInfo;
import com.sakame.model.dto.ApplyMsg;
import com.sakame.proxy.ServiceProxyFactory;
import com.sakame.registry.Registry;
import com.sakame.registry.RegistryFactory;
import com.sakame.server.VertxHttpServer;
import com.sakame.service.RaftService;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.*;

/**
 * @author sakame
 * @version 1.0
 */
@Slf4j
public class RaftApplication {

    private RaftConfig config = RaftConfig.getInstance();

    private Registry registry;

    private Random random = new Random();

    /**
     * 初始化 raft 应用，击中管理所有 raft 并提供相关操作方法
     * @param n
     * @param unreliable
     * @param snapshot
     */
    public void init(int n, boolean unreliable, boolean snapshot) {
        config.setRaftCount(n);
        config.setRafts(new Raft[n]);
        config.setConnected(new boolean[n]);
        config.setSaved(new Persister[n]);
        config.setLogs(new Map[n]);
        config.setStartTime(LocalDateTime.now());
        config.setServers(new VertxHttpServer[n]);
        config.setPeers(new RaftService[n]);
        config.setServices(new ServiceMetaInfo[n]);
        ServiceMetaInfo[] services = config.getServices();
        RaftService[] peers = config.getPeers();
        int port = 22222;

        // 1.初始化 serviceMetaInfo 并注册服务
        RpcConfig rpcConfig = RpcConfig.getRpcConfig();
        RegistryConfig registryConfig = rpcConfig.getRegistryConfig();
        registry = RegistryFactory.getInstance(registryConfig.getRegistry());
        for (int j = 0; j < n; j++) {
            ServiceMetaInfo serviceMetaInfo = new ServiceMetaInfo();
            serviceMetaInfo.setServiceName(RaftService.class.getName());
            serviceMetaInfo.setServiceVersion(RpcConstant.DEFAULT_SERVICE_VERSION);
            serviceMetaInfo.setServiceHost(rpcConfig.getServerHost());
            serviceMetaInfo.setServicePort(++port);
            services[j] = serviceMetaInfo;
            try {
                registry.register(serviceMetaInfo);
            } catch (Exception e) {
                throw new RuntimeException("fail to start raft", e);
            }
        }

        // 2.初始化 rpc 服务调用传给所有的 raft
        for (int i = 0; i < n; i++) {
            peers[i] = ServiceProxyFactory.getProxy(RaftService.class, i);
        }

        // 3.实例化 raft 分配单例给 server,并启动 server
        for (int i = 0; i < n; i++) {
            start(i, snapshot);
        }

        // 4.初始化 init (开启其 ticker 线程)
        for (int i = 0; i < n; i++) {
            config.getConnected()[i] = true;
            config.getRafts()[i].init(config.getPeers(), i, config.getSaved()[i], new ApplyMsg());
        }
    }

    /**
     * 关闭某一个 raft
     * @param i
     */
    public void shutdown(int i) {

    }

    /**
     * 读取通道提交的消息
     * @param i
     * @param applyMsg
     */
    public void applier(int i, ApplyMsg applyMsg) {
        if (applyMsg.isCommandValid()) {
            config.getLock().lock();
            boolean result = checkLogs(i, applyMsg);
            if (!result) {
                log.error("fail to apply message:{} to server:{}", applyMsg, i);
            }
            config.getLock().unlock();
        }
    }

    public void applierSnap(int i, ApplyMsg applyMsg) {

    }

    /**
     * 检查日志条目和提交内容是否匹配并修改
     * @param i
     * @param applyMsg
     * @return
     */
    public boolean checkLogs(int i, ApplyMsg applyMsg) {
        Object command = applyMsg.getCommand();
        int index = applyMsg.getCommandIndex();
        Map<Integer, Object>[] logs = config.getLogs();
        for (int j = 0; j < logs.length; j++) {
            if (command != logs[j].get(index)) {
                log.warn("check log:{}, != server log:{}", logs[i], logs[j]);
            }
        }
        boolean key = logs[i].containsKey(index - 1);
        logs[i].put(index, command);
        if (index > config.getMaxIndex()) {
            config.setMaxIndex(index);
        }
        return key;
    }

    /**
     * 检查是否只有一个 leader
     * @return
     */
    public int checkOneLeader() {
        for (int i = 0; i < 10; i++) {
            int interval = 450 + random.nextInt(100);
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            Raft[] rafts = config.getRafts();
            Map<Integer, List<Integer>> leaders = new HashMap<>();
            for (int j = 0; j < config.getRaftCount(); j++) {
                if (!rafts[j].killed() && rafts[j].isLeader()) {
                    if (!leaders.containsKey(rafts[j].getTerm())) {
                        leaders.put(rafts[j].getTerm(), new ArrayList<>());
                    }
                    List<Integer> list = leaders.get(rafts[j].getTerm());
                    list.add(j);
                }
            }

            int lastTermLeader = -1;
            for (Map.Entry<Integer, List<Integer>> kv : leaders.entrySet()) {
                int term = kv.getKey();
                if (kv.getValue().size() > 1) {
                    log.error("term {} has {} leaders", term, kv.getValue());
                }
                if (term > lastTermLeader) {
                    lastTermLeader = term;
                }
            }

            if (leaders.size() != 0) {
                return leaders.get(lastTermLeader).get(0);
            }
        }
        log.error("expected one leader, but got none");
        return -1;
    }

    public void checkNoLeader() {
        for (int i = 0; i < config.getRaftCount(); i++) {
            if (config.getConnected()[i] && config.getRafts()[i].isLeader()) {
                log.error("expected no leader but got one:raft{}", i);
            }
        }
    }

    public int checkTerms() {
        int term = -1;
        int n = config.getRaftCount();
        for (int i = 0; i < n; i++) {
            if (config.getConnected()[i]) {
                int xterm = config.getRafts()[i].getTerm();
                if (term == -1) {
                    term = xterm;
                } else if (xterm != term) {
                    log.error("servers disagree on term");
                }
            }
        }
        return term;
    }

    /**
     * 选择一个 raft 注册到注册中心
     * @param i
     */
    public void connect(int i) {
        config.getConnected()[i] = true;
        config.getServers()[i].doStart(config.getServices()[i].getServicePort());
        config.getRafts()[i].restart();
    }

    /**
     * 选择一个 raft 断开连接
     * @param i
     */
    public void disconnect(int i) {
        config.getConnected()[i] = false;
        config.getServers()[i].doShutdown();
        config.getRafts()[i].kill();
    }

    /**
     * 关闭一台 raft
     * @param i
     */
    public void crash(int i) {
        if (config.getConnected()[i]) {
            disconnect(i);
            config.getRafts()[i].kill();
        }
    }

    /**
     * 启动一个 raft 服务，开启服务器
     * @param i
     * @param snapshot
     */
    public void start(int i, boolean snapshot) {
        crash(i);
        config.getLock().lock();

        if (config.getSaved()[i] == null) {
            config.getSaved()[i] = new Persister();
        }
        ServiceMetaInfo[] services = config.getServices();

        Raft raft = new Raft();
        ApplyMsg applyMsg = new ApplyMsg();
        config.getRafts()[i] = raft;
        VertxHttpServer server = new VertxHttpServer(raft);
        server.doStart(services[i].getServicePort());
        config.getServers()[i] = server;
        if (snapshot) {
            new Thread(() -> applierSnap(i, applyMsg));
        } else {
            new Thread(() -> applier(i, applyMsg));
        }

        config.getLock().unlock();
    }

}
