package com.sakame;

import com.sakame.config.RaftConfig;
import com.sakame.config.RegistryConfig;
import com.sakame.config.RpcConfig;
import com.sakame.constant.RpcConstant;
import com.sakame.model.Channel;
import com.sakame.model.ServiceMetaInfo;
import com.sakame.model.dto.ApplyMsg;
import com.sakame.proxy.ServiceProxyFactory;
import com.sakame.registry.Registry;
import com.sakame.registry.RegistryFactory;
import com.sakame.server.VertxHttpServer;
import com.sakame.service.RaftService;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
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
     *
     * @param n
     * @param unreliable
     * @param snapshot
     */
    public void init(int n, boolean unreliable, boolean snapshot) {
        config.setRaftCount(n);
        config.setRafts(new Raft[n]);
        config.setConnected(new boolean[n]);
        config.setSaved(new Persister[n]);
        config.setLogs(new HashMap[n]);
        config.setStartTime(LocalDateTime.now());
        config.setServers(new VertxHttpServer[n]);
        config.setPeers(new RaftService[n]);
        config.setServices(new ServiceMetaInfo[n]);
        ServiceMetaInfo[] services = config.getServices();
        RaftService[] peers = config.getPeers();
        Map<Integer, Object>[] logs = config.getLogs();
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
            logs[j] = new HashMap<>();
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
            startServer(i);
        }

        // 4.初始化 raft (开启其 ticker 线程)
        for (int i = 0; i < n; i++) {
            startRaft(i, snapshot);
        }
    }

    /**
     * 关闭某一个 raft
     *
     * @param i
     */
    public void shutdown(int i) {

    }

    /**
     * application 自己维护一个 logs map 数组而不直接访问 raft 的 logs
     * 通过多线程读取 raft 通道已提交日志信息来更新 logs
     *
     * @param i
     * @param channel
     */
    public void applier(int i, Channel<ApplyMsg> channel) {
        while (true) {
            // 循环读，有数据则更新，无数据则等待
            ApplyMsg applyMsg = channel.readOne();
            if (applyMsg.isCommandValid()) {
                config.getLock().lock();
                boolean result = checkLogs(i, applyMsg);
                if (!result) {
                    log.error("fail to apply message:{} to server:{}", applyMsg, i);
                }
                config.getLock().unlock();
            }
        }
    }

    public void applierSnap(int i, Channel<ApplyMsg> channel) {

    }

    /**
     * 检查日志条目和提交内容是否匹配并修改
     *
     * @param i
     * @param applyMsg
     * @return
     */
    public boolean checkLogs(int i, ApplyMsg applyMsg) {
        Object command = applyMsg.getCommand();
        int index = applyMsg.getCommandIndex();
        Map<Integer, Object>[] logs = config.getLogs();
        for (int j = 0; j < logs.length; j++) {
            if (command != null && i != j && !logs[j].getOrDefault(index, -1).equals(command)) {
                log.warn("raft:{}:logs:{} != raft:{}:logs:{}", i, logs[i], j, logs[j]);
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
     *
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

    /**
     * 检查是否不存在 leader
     */
    public void checkNoLeader() {
        for (int i = 0; i < config.getRaftCount(); i++) {
            if (config.getConnected()[i] && config.getRafts()[i].isLeader()) {
                log.error("expected no leader but got one:raft{}", i);
            }
        }
    }

    /**
     * 检查是否所有 raft 所处的 term 一致
     *
     * @return
     */
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
     * 检查某日志索引的提交数和对应的命令
     *
     * @param index
     * @return
     */
    public Object[] nCommitted(int index) {
        int cnt = 0;
        Object cmd = null;
        int n = config.getRaftCount();
        for (int i = 0; i < n; i++) {
            if (config.getLogs()[i].containsKey(index)) {
                if (cnt > 0 && !config.getLogs()[i].get(index).equals(cmd)) {
                    log.error("committed values do not match index:{}, expected {} but got {}", index, cmd, config.getLogs()[i].get(index));
                }
                cnt++;
                cmd = config.getLogs()[i].get(index);
            }
        }
        return new Object[]{cnt, cmd};
    }

    /**
     * wait for at least n servers to commit.
     *
     * @param index
     * @param n
     * @param startTerm
     * @return
     */
    public Object wait(int index, int n, int startTerm) {
        int to = 10;
        for (int i = 0; i < 30; i++) {
            int cnt = (int) nCommitted(index)[0];
            if (cnt >= n) {
                break;
            }
            try {
                Thread.sleep(to);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (to < 1000) {
                to *= 2;
            }
            if (startTerm > -1) {
                for (Raft raft : config.getRafts()) {
                    if (raft.getTerm() > startTerm) {
                        return -1;
                    }
                }
            }
        }
        Object[] objects = nCommitted(index);
        if ((int) objects[0] < n) {
            log.error("only {} decided for index {}; wanted {}", objects[0], index, n);
        }
        return objects[1];
    }

    /**
     * 给指定个数的 raft 新增一条日志(实际只给 leader 加，但是会检查是否有指定个 raft 已经追加日志)
     *
     * @param cmd
     * @param expectedServers
     * @param retry
     * @return 返回增加一条日志后该日志对应的索引值
     */
    public int one(Object cmd, int expectedServers, boolean retry) {
        LocalDateTime time1 = LocalDateTime.now().plus(Duration.ofSeconds(10));
        while (LocalDateTime.now().isBefore(time1)) {
            int index = -1;
            int n = config.getRaftCount();
            // 遍历每一个 raft 新增一个日志
            for (int i = 0; i < n; i++) {
                config.getLock().lock();
                if (config.getConnected()[i]) {
                    int index1 = config.getRafts()[i].startCmd(cmd);
                    if (index1 != -1) {
                        index = index1;
                        config.getLock().unlock();
                        break;
                    }
                }
                config.getLock().unlock();
            }

            // 如果提交成功
            if (index != -1) {
                LocalDateTime time2 = LocalDateTime.now().plus(Duration.ofSeconds(2));
                while (LocalDateTime.now().isBefore(time2)) {
                    // 获取要求新增的日志有多少 raft 新增成功
                    int cnt = (int) nCommitted(index)[0];
                    Object cmd1 = nCommitted(index)[1];
                    log.info("{} rafts applied cmd:{}", cnt, cmd1);
                    // 如果成功新增日志的 raft 的个数满足期望的值则返回结束
                    if (cnt >= expectedServers && cmd1.equals(cmd)) {
                        return index;
                    }
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                if (!retry) {
                    log.error("cmd:{} failed to reach agreement", cmd);
                }
            } else {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        log.error("cmd:{} failed to reach agreement", cmd);
        return -1;
    }

    /**
     * 选择一个 raft 注册到注册中心
     *
     * @param i
     */
    public void connect(int i) {
        if (!config.getConnected()[i]) {
            log.info("connect raft:{}", i);
            config.getConnected()[i] = true;
            config.getServers()[i].doStart(config.getServices()[i].getServicePort());
            config.getRafts()[i].restart();
        }
    }

    /**
     * 选择一个 raft 断开连接
     *
     * @param i
     */
    public void disconnect(int i) {
        if (config.getConnected()[i]) {
            log.info("disconnect raft:{}", i);
            config.getConnected()[i] = false;
            config.getServers()[i].doShutdown();
            // 通过执行 killed 函数来停掉 raft 的所有线程（线程基于 killed 循环判断）
            config.getRafts()[i].kill();
        }
    }

    /**
     * 退出清除所有的 raft
     */
    public void cleanup() {
        int n = config.getRaftCount();
        // raft 的启动归为两类，都关闭即可正常退出
        // 1.基于 killed() 判断的线程
        // 2.server 线程
        for (int i = 0; i < n; i++) {
            config.getRafts()[i].kill();
            config.getServers()[i].doShutdown();
            // 注册也要解除一下，因为线程关闭后要等 30 秒才会自己删除
            registry.unRegister(config.getServices()[i]);
        }
    }

    /**
     * 关闭一台 raft
     *
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
     *
     * @param i
     */
    public void startServer(int i) {
        crash(i);
        config.getLock().lock();

        if (config.getSaved()[i] == null) {
            config.getSaved()[i] = new Persister();
        }
        ServiceMetaInfo[] services = config.getServices();

        Raft raft = new Raft();
        config.getRafts()[i] = raft;
        VertxHttpServer server = new VertxHttpServer(raft);
        server.doStart(services[i].getServicePort());
        config.getServers()[i] = server;

        config.getLock().unlock();
    }

    /**
     * 用于第一次启动一个 raft
     *
     * @param i
     * @param snapshot
     */
    public void startRaft(int i, boolean snapshot) {
        Channel<ApplyMsg> channel = new Channel<>();
        config.getLock().lock();
        config.getConnected()[i] = true;
        Raft[] rafts = config.getRafts();
        rafts[i].init(config.getPeers(), i, config.getSaved()[i], channel);

        if (snapshot) {
            new Thread(() -> applierSnap(i, channel));
        } else {
            new Thread(() -> applier(i, channel)).start();
        }
        config.getLock().unlock();
    }

    /**
     * 获取某一个 raft
     *
     * @param i
     * @return
     */
    public Raft getRaft(int i) {
        return config.getRafts()[i];
    }

}
