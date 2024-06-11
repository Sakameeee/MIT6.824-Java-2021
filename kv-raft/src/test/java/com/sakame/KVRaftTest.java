package com.sakame;

import cn.hutool.core.date.StopWatch;
import cn.hutool.core.util.ArrayUtil;
import com.sakame.model.Channel;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/**
 * @author sakame
 * @version 1.0
 */
@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KVRaftTest {

    private static final Random random = new Random();

    private static final int ELECTION_TIMEOUT = 1000;

    /**
     * 生成一个客户端并等待是否成功
     *
     * @param application
     * @param clients
     * @param fn
     */
    void spawnClientsAndWait(KVRaftApplication application, int clients, BiFunction<Integer, Client, Void> fn) {
        Channel<Boolean>[] ca = new Channel[clients];
        for (int i = 0; i < clients; i++) {
            ca[i] = new Channel<>();
            final int me = i;
            new Thread(() -> runClient(application, me, ca[me], fn)).start();
        }
        for (int i = 0; i < clients; i++) {
            Boolean ok = ca[i].readOne();
            if (!ok) {
                log.error("fail to start client {}", i);
            }
        }
    }

    /**
     * 运行客户端，调用函数式接口并在一定时间内反复通过客户端执行三种操作
     *
     * @param application
     * @param me
     * @param ca
     * @param fn
     */
    void runClient(KVRaftApplication application, int me, Channel<Boolean> ca, BiFunction<Integer, Client, Void> fn) {
        boolean ok = false;
        Client client = application.makeClient();
        fn.apply(me, client);
        ok = true;
        ca.writeOne(ok);
    }

    String nextValue(String last, String value) {
        return last + value;
    }

    /**
     * 在信号量为 0 时一直制造分区
     * 每一次制造的分区都是新的不受上一次干扰
     * 信号量为 1 时结束并通过通道通知结束消息
     *
     * @param application
     * @param ch
     * @param done
     */
    void partitioner(KVRaftApplication application, Channel<Boolean> ch, AtomicInteger done) {
        while (done.get() == 0) {
            int n = application.getNServers();
            int[] a = new int[n];
            for (int i = 0; i < n; i++) {
                a[i] = random.nextInt(10) % 2;
            }
            Integer[][] pa = new Integer[2][];
            for (int i = 0; i < 2; i++) {
                pa[i] = new Integer[0];
                for (int j = 0; j < n; j++) {
                    if (a[j] == i) {
                        pa[i] = ArrayUtil.append(pa[i], j);
                    }
                }
            }
            int[] p1 = Arrays.stream(pa[0]).mapToInt(Integer::intValue).toArray();
            int[] p2 = Arrays.stream(pa[1]).mapToInt(Integer::intValue).toArray();
            application.partition(p1, p2);
        }
        ch.writeOne(true);
    }

    /**
     * 检查每一次 append 的值是否正确被记录
     *
     * @param client
     * @param value
     * @param count
     */
    boolean checkClntAppends(int client, String value, int count) {
        int lastOff = -1;
        boolean ok = true;
        for (int i = 0; i < count; i++) {
            String wanted = "x " + client + " " + i + " y";
            int off = value.indexOf(wanted, lastOff);
            if (off < 0) {
                log.error("{} missing element {} in Append result {}", client, wanted, value);
                ok = false;
            }
            int off1 = value.lastIndexOf(wanted);
            if (off1 != off) {
                log.error("duplicate element {} in Append result", wanted);
                ok = false;
            }
            if (off <= lastOff) {
                log.error("wrong order for element {} in Append result", wanted);
                ok = false;
            }
            lastOff = off;
        }
        return ok;
    }

    /**
     * 检查某个客户端根据 key 获取到的值是否符合预期
     *
     * @param client
     * @param key
     * @param value
     * @return
     */
    boolean check(Client client, String key, String value) {
        String ret = client.get(key);
        return ret.equals(value);
    }

    void genericTest(String part, int clients, int servers, boolean crash, boolean partitions, int maxRaftState, boolean randomKeys) throws InterruptedException {
        String title = "Test: ";
        if (crash) {
            title += "restarts, ";
        }
        if (partitions) {
            title += "partitions, ";
        }
        if (maxRaftState != -1) {
            title += "snapshots, ";
        }
        if (randomKeys) {
            title += "random keys, ";
        }
        if (clients > 1) {
            title += "many clients";
        } else {
            title += "one client";
        }
        title += " (" + part + ")";
        log.info(title);

        KVRaftApplication application = new KVRaftApplication();
        application.init(servers, maxRaftState);
        Client client1 = application.makeClient();

        // 控制制造分区的停止
        AtomicInteger done_partitioner = new AtomicInteger(0);
        // 控制客户端行为的定制
        AtomicInteger done_clients = new AtomicInteger(0);
        // 每个客户端对应一个通道，只写入一个信息即操作数
        Channel<Integer>[] clnts = new Channel[clients];
        Channel<Boolean> partitionerCh = new Channel<>();
        for (int i = 0; i < clients; i++) {
            clnts[i] = new Channel<>();
        }
        for (int i = 0; i < 3; i++) {
            done_clients.set(0);
            done_partitioner.set(0);
            BiFunction<Integer, Client, Void> function = (me, client) -> {
                // cnt 是客户端往服务端操作的次数
                int cnt = 0;
                // last 记录 randomKeys 不为真时每一次 append 的值的拼接
                String last = "";
                if (!randomKeys) {
                    client.put(String.valueOf(me), last);
                }
                while (done_clients.get() == 0) {
                    // 如果 randomKeys 不为真则 key 取自身编号
                    String key;
                    if (randomKeys) {
                        key = String.valueOf(random.nextInt(clients));
                    } else {
                        key = String.valueOf(me);
                    }
                    String value = "x " + me + " " + cnt + " y";
                    // randomKeys 不为真时只会 append 和 get
                    if (random.nextInt(Integer.MAX_VALUE) % 1000 < 500) {
                        client.append(key, value);
                        if (!randomKeys) {
                            last = nextValue(last, value);
                        }
                        cnt++;
                    } else if (randomKeys && (random.nextInt(Integer.MAX_VALUE) % 1000 < 100)) {
                        client.put(key, value);
                        cnt++;
                    } else {
                        String ret = client.get(key);
                        // last 的拼接值应该和客户端获取的最新值匹配
                        Assertions.assertFalse(!randomKeys && !ret.equals(last));
                    }
                }
                clnts[me].writeOne(cnt);
                return null;
            };
            new Thread(() -> spawnClientsAndWait(application, clients, function)).start();

            if (partitions) {
                Thread.sleep(1000);
                new Thread(() -> partitioner(application, partitionerCh, done_partitioner)).start();
            }
            Thread.sleep(3000);

            done_clients.set(1);
            done_partitioner.set(1);

            if (partitions) {
                partitionerCh.readOne();
                application.connectAll();
                Thread.sleep(ELECTION_TIMEOUT);
            }

            if (crash) {
                for (int j = 0; j < servers; j++) {
                    application.shutdownServer(j);
                }
                Thread.sleep(ELECTION_TIMEOUT);
                for (int j = 0; j < servers; j++) {
                    application.startServer(j);
                }
                application.connectAll();
            }

            for (int j = 0; j < clients; j++) {
                int count = clnts[j].readOne();
                // 拿取最终的值
                String value = client1.get(String.valueOf(j));
                if (!randomKeys) {
                    Assertions.assertTrue(checkClntAppends(j, value, count));
                }
            }

            // 开启 snapshot 检查
            if (maxRaftState > 0) {
                int size = application.logSize();
                if (size > 8 * maxRaftState) {
                    log.error("logs were not trimmed");
                }
            }

            // 不开启 snapshot 检查
            if (maxRaftState < 0) {
                int size = application.snapshotSize();
                if (size > 0) {
                    log.error("snapshot should not be used");
                }
            }
        }
        application.cleanup();
    }

    void genericTestSpeed(String part, int maxRaftState) {
        final int servers = 3;
        final int numOps = 100;
        KVRaftApplication application = new KVRaftApplication();
        application.init(servers, maxRaftState);
        Client client = application.makeClient();

        log.info("Test: ops complete fast enough {}", part);

        StopWatch stopWatch = new StopWatch();
        client.put("x", "");
        stopWatch.start();
        for (int i = 0; i < numOps; i++) {
            client.append("x", "x 0 " + i + " y");
        }
        stopWatch.stop();

        final int heartbeatInterval = 100;
        final int opsPerInterval = 3;
        final int timePerOp = heartbeatInterval / opsPerInterval;
        log.info("time per operation:{}, time per operation expected {}", stopWatch.getLastTaskTimeMillis() / numOps, timePerOp);

        application.cleanup();
    }

    @Test
    @Order(1)
    void testBasic3A() throws InterruptedException {
        genericTest("3A", 1, 5, false, false, -1, false);
    }

    @Test
    @Order(2)
    void testSpeed3A() {
        genericTestSpeed("3A", -1);
    }

    @Test
    @Order(3)
    void testConcurrent3A() throws InterruptedException {
        genericTest("3A", 5, 5, false, false, -1, false);
    }

    @Test
    @Order(4)
    void testOnePartition3A() throws InterruptedException {
        final int servers = 5;
        KVRaftApplication application = new KVRaftApplication();
        application.init(servers, -1);
        Client client = application.makeClient();

        log.info("Test: progress in majority (3A)");

        client.put("1", "13");
        int[][] ints = application.makePartition();
        application.partition(ints[0], ints[1]);

        // client1 连接到 p1
        Client client1 = application.makeClient();
        application.disconnectClient(client1, ints[1]);
        // client2a 连接到 p2
        Client client2a = application.makeClient();
        application.disconnectClient(client2a, ints[0]);
        // client2b 连接到 p2
        Client client2b = application.makeClient();
        application.disconnectClient(client2b, ints[0]);
        // client1 的操作能成功
        client1.put("1", "14");
        Assertions.assertTrue(check(client1, "1", "14"));

        log.info("Test: no progress in minority (3A)");
        Channel<Boolean> done0 = new Channel<>(1);
        Channel<Boolean> done1 = new Channel<>(1);
        new Thread(() -> {
            client2a.put("1", "15");
            done0.write(true);
        }).start();
        new Thread(() -> {
            client2b.get("1");
            done1.write(true);
        }).start();
        // client2a 和 client2b 的操作会失败
        Boolean ret;
        Assertions.assertNull(ret = done0.read());
        Assertions.assertNull(ret = done1.read());

        Assertions.assertTrue(check(client1, "1", "14"));
        client1.put("1", "16");
        Assertions.assertTrue(check(client1, "1", "16"));

        log.info("Test: completion after heal (3A)");
        // 恢复所有的连接
        application.connectAll();
        application.connectClient(client2a, application.all());
        application.connectClient(client2b, application.all());
        Thread.sleep(100);
        Assertions.assertTrue(check(client, "1", "15"));

        application.cleanup();
    }

    @Test
    @Order(5)
    void testManyPartitionsOneClient3A() throws InterruptedException {
        genericTest("3A", 1, 5, false, true, -1, false);
    }

    @Test
    @Order(6)
    void testManyPartitionsManyClients3A() throws InterruptedException {
        genericTest("3A", 5, 5, false, true, -1, false);
    }

    @Test
    @Order(7)
    void testPersistOneClient3A() throws InterruptedException {
        genericTest("3A", 1, 5, true, false, -1, false);
    }

    @Test
    @Order(8)
    void testPersistConcurrent3A() throws InterruptedException {
        genericTest("3A", 5, 5, true, false, -1, false);
    }

    @Test
    @Order(9)
    void testPersistPartition3A() throws InterruptedException {
        genericTest("3A", 5, 5, true, true, -1, false);
    }

    @Test
    void testSnapshotRPC3B() throws InterruptedException {
        final int servers = 3;
        int maxRaftState = 1000;
        KVRaftApplication application = new KVRaftApplication();
        application.init(servers, maxRaftState);

        Client client = application.makeClient();
        log.info("Test: InstallSnapshot RPC (3B)");

        client.put("a", "A");
        Assertions.assertTrue(check(client, "a", "A"));

        // 向大分区中提交大量命令
        application.partition(new int[]{0, 1}, new int[]{2});
        {
            Client client1 = application.makeClient();
            application.disconnectClient(client1, new int[]{2});
            for (int i = 0; i < 50; i++) {
                client1.put(String.valueOf(i), String.valueOf(i));
            }
            Thread.sleep(ELECTION_TIMEOUT);
            client1.put("b", "B");
        }

        // 检查日志是否正确裁切
        int size = application.logSize();
        Assertions.assertTrue(size < 8 * maxRaftState);

        // 将之前小分区的 server 放到大分区中，检查是否正常日志追赶
        application.partition(new int[]{0, 2}, new int[]{1});
        {
            Client client1 = application.makeClient();
            application.disconnectClient(client1, new int[]{1});
            client1.put("c", "C");
            client1.put("d", "D");
            Assertions.assertTrue(check(client1, "a", "A"));
            Assertions.assertTrue(check(client1, "b", "B"));
            Assertions.assertTrue(check(client1, "1", "1"));
            Assertions.assertTrue(check(client1, "49", "49"));
        }

        // 恢复所有连接
        application.partition(new int[]{1, 2, 3}, new int[]{});
        client.put("e", "E");
        Assertions.assertTrue(check(client, "c", "C"));
        Assertions.assertTrue(check(client, "e", "E"));
        Assertions.assertTrue(check(client, "1", "1"));

        application.cleanup();
    }

    @Test
    void testSnapshotSize3B() {
        final int servers = 3;
        int maxRaftState = 1000;
        int maxSnapshotState = 500;
        KVRaftApplication application = new KVRaftApplication();
        application.init(servers, maxRaftState);

        Client client = application.makeClient();
        log.info("Test: snapshot size is reasonable (3B)");

        for (int i = 0; i < 200; i++) {
            client.put("x", "0");
            Assertions.assertTrue(check(client, "x", "0"));
            client.put("x", "1");
            Assertions.assertTrue(check(client, "x", "1"));
        }

        Assertions.assertTrue(application.snapshotSize() < maxSnapshotState);
        Assertions.assertTrue(application.logSize() < maxRaftState * 8);

        application.cleanup();
    }

    @Test
    void testSpeed3B() {
        genericTestSpeed("3B", 1000);
    }

    @Test
    void testSnapshotRecover3B() throws InterruptedException {
        genericTest("3B", 1, 5, true, false, 1000, false);
    }

    @Test
    void testSnapshotRecoverManyClients3B() throws InterruptedException {
        genericTest("3B", 20, 5, true, false, 1000, false);
    }

    @Test
    void testSnapshotUnreliable3B() throws InterruptedException {
        genericTest("3B", 5, 5, false, false, 1000, false);
    }

    @Test
    void testSnapshotUnreliableRecover3B() throws InterruptedException {
        genericTest("3B", 5, 5, true, false, 1000, false);
    }

    @Test
    void testSnapshotUnreliableRecoverConcurrentPartition3B() throws InterruptedException {
        genericTest("3B", 5, 5, true, true, 1000, false);
    }

    @Test
    void testSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B() throws InterruptedException {
        genericTest("3B", 15, 7, true, true, 1000, true);
    }

}
