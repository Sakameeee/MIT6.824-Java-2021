package com.sakame;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ArrayUtil;
import com.sakame.model.Channel;
import com.sakame.model.dto.ApplyMsg;
import com.sakame.serializer.Serializer;
import com.sakame.serializer.SerializerFactory;
import com.sakame.serializer.SerializerKeys;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author sakame
 * @version 1.0
 */
@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RaftTest {

    private static final int MAX_LOG_SIZE = 2000;

    private static final int SNAPSHOT_INTERVAL = 10;

    @Test
    @Order(1)
    void testInitialElection2A() throws InterruptedException {
        int servers = 3;
        RaftApplication application = new RaftApplication();
        application.init(servers, false, false);
        log.info("Test (2A): initial election");

        Thread.sleep(50);
        int leader1 = application.checkOneLeader();

        Thread.sleep(50);
        int term1 = application.checkTerms();

        Thread.sleep(50);
        int term2 = application.checkTerms();

        int leader2 = application.checkOneLeader();

        Assertions.assertNotEquals(-1, leader2);
        Assertions.assertEquals(leader1, leader2);
        Assertions.assertEquals(term1, term2);

        application.cleanup();
    }

    @Test
    @Order(2)
    void testReElection2A() throws InterruptedException {
        int servers = 3;
        RaftApplication application = new RaftApplication();
        application.init(servers, false, false);
        log.info("Test (2A): election after network failure");

        Thread.sleep(50);
        int leader1 = application.checkOneLeader();

        application.disconnect(leader1);
        Thread.sleep(50);

        int leader2 = application.checkOneLeader();
        application.connect(leader1);
        int leader3 = application.checkOneLeader();

        application.disconnect(leader2);
        application.disconnect((leader2 + 1) % servers);
        Thread.sleep(50);
        application.checkNoLeader();

        application.connect((leader2 + 1) % servers);
        int leader4 = application.checkOneLeader();

        application.connect(leader2);
        int leader5 = application.checkOneLeader();

        Assertions.assertNotEquals(-1, leader1);
        Assertions.assertEquals(leader2, leader3);
        Assertions.assertNotEquals(-1, leader4);
        Assertions.assertEquals(leader4, leader5);

        application.cleanup();
    }

    @Test
    @Order(3)
    void testBasicAgree2B() throws InterruptedException {
        int servers = 3;
        RaftApplication application = new RaftApplication();
        application.init(servers, false, false);
        log.info("Test (2B): basic agreement");

        Thread.sleep(50);
        int iters = 3;
        for (int index = 1; index < iters + 1; index++) {
            int cnt = (int) application.nCommitted(index)[0];
            if (cnt > 0) {
                log.error("some have committed before start()");
            }

            // 给 servers 个 raft 新增一条日志
            int one = application.one(index * 100, servers, false);
            Assertions.assertEquals(index, one);
        }

        application.cleanup();
    }

    @Test
    @Order(4)
    void testFailAgree2B() throws InterruptedException {
        int servers = 3;
        RaftApplication application = new RaftApplication();
        application.init(servers, false, false);
        log.info("Test (2B): agreement despite follower disconnection");

        Thread.sleep(50);
        // 新增一条日志
        application.one(101, servers, false);

        // 关闭一个 follower 后继续增加日志
        int leader = application.checkOneLeader();
        application.disconnect((leader + 1) % servers);
        application.one(102, servers - 1, false);
        application.one(103, servers - 1, false);
        application.one(104, servers - 1, false);
        application.one(105, servers - 1, false);

        // 重新连接该 follower
        application.connect((leader + 1) % servers);
        application.one(106, servers, false);
        int index = application.one(107, servers, false);
        Object[] objects = application.nCommitted(index);

        Assertions.assertEquals(7, index);
        Assertions.assertEquals(servers, objects[0]);

        application.cleanup();
    }

    @Test
    @Order(5)
    void testFailNoAgree2B() throws InterruptedException {
        int servers = 5;
        RaftApplication application = new RaftApplication();
        application.init(servers, false, false);
        log.info("Test (2B): no agreement if too many followers disconnect");

        Thread.sleep(50);
        application.one(10, servers, false);
        int leader = application.checkOneLeader();
        application.disconnect((leader + 1) % servers);
        application.disconnect((leader + 2) % servers);
        application.disconnect((leader + 3) % servers);

        // 新增一个会复制但不会提交的日志
        int index = application.getRaft(leader).startCmd(20);
        Assertions.assertEquals(2, index);

        Thread.sleep(50);
        // 由于没有超过半数的 rafts 在线，提交数应该为 0
        int cnt = (int) application.nCommitted(index)[0];
        Assertions.assertEquals(0, cnt);

        application.connect((leader + 1) % servers);
        application.connect((leader + 2) % servers);
        application.connect((leader + 3) % servers);

        int leader1 = application.checkOneLeader();
        int index1 = application.getRaft(leader1).startCmd(20);
        Thread.sleep(50);
        int cnt1 = (int) application.nCommitted(index1)[0];
        Assertions.assertTrue(index1 == 2 || index1 == 3);
        Assertions.assertEquals(servers, cnt1);

        application.cleanup();
    }

    @Test
    @Order(6)
    void testConcurrentStarts2B() throws InterruptedException, ExecutionException {
        int servers = 3;
        RaftApplication application = new RaftApplication();
        application.init(servers, false, false);
        log.info("Test (2B): concurrent Start()s");

        Thread.sleep(50);
        boolean success = false;
        loop:
        for (int i = 0; i < 5; i++) {
            if (i > 0) {
                Thread.sleep(3000);
            }

            int leader = application.checkOneLeader();
            if (application.getRaft(leader).startCmd(1) == -1) {
                continue;
            }
            int term = application.checkTerms();

            // 测试对 leader 的并发操作写入
            int iters = 5;
            CountDownLatch latch = new CountDownLatch(5);
            Channel<Integer> is = new Channel<>(iters);
            for (int j = 0; j < iters; j++) {
                int finalJ = j;
                CompletableFuture.runAsync(() -> {
                    int index = application.getRaft(leader).startCmd(100 + finalJ);
                    if (index == -1) {
                        return;
                    }
                    is.write(index);
                    log.info("writes index {}", index);
                    latch.countDown();
                }).get();
            }

            latch.await();
            Thread.sleep(100);

            for (int j = 0; j < servers; j++) {
                if (application.getRaft(j).getTerm() != term) {
                    continue loop;
                }
            }

            boolean failed = false;
            Integer index;
            Object[] cmds = new Object[0];
            while ((index = is.read()) != null) {
                Object cmd = application.wait(index, servers, term);
                if ((int) cmd == -1) {
                    failed = true;
                    break;
                }
                cmds = ArrayUtil.append(cmds, cmd);
                log.info("cmds:{}, index:{}, cmd:{}", cmds, index, cmd);
            }

            if (failed) {
                CompletableFuture.runAsync(() -> {
                    while (is.read() != null) ;
                }).get();
            }

            Object[] expected = new Object[]{100, 101, 102, 103, 104};
            Arrays.sort(cmds);
            Assertions.assertArrayEquals(expected, cmds);

            success = true;
            break;
        }

        if (!success) {
            log.error("term changed too often");
        }

        application.cleanup();
    }

    @Test
    @Order(7)
    void testRejoin2B() throws InterruptedException {
        int servers = 3;
        RaftApplication application = new RaftApplication();
        application.init(servers, false, false);
        log.info("Test (2B): rejoin of partitioned leader");

        Thread.sleep(50);
        application.one(101, servers, false);

        // 给第一任 leader 断连并独自更新日志
        int leader = application.checkOneLeader();
        application.disconnect(leader);
        Thread.sleep(100);
        application.getRaft(leader).startCmd(102);
        application.getRaft(leader).startCmd(103);
        application.getRaft(leader).startCmd(104);

        // 给第二任 leader 增加日志后断连，再将第一任 leader 重连
        application.one(103, servers - 1, true);
        int leader1 = application.checkOneLeader();
        application.disconnect(leader1);
        application.connect(leader);

        Thread.sleep(50);
        int index = application.one(104, servers - 1, true);
        // 再将第二任 leader 重连
        application.connect(leader1);

        Thread.sleep(100);
        // 判断日志是否正常更新
        Assertions.assertEquals(3, index);
        Object[] objects = application.nCommitted(index);
        // 判断第一任的日志是否被正确覆写
        Assertions.assertEquals(servers, objects[0]);
        Assertions.assertEquals(104, objects[1]);

        application.cleanup();
    }

    @Test
    @Order(8)
    void testBackup2B() throws InterruptedException {
        int servers = 5;
        RaftApplication application = new RaftApplication();
        application.init(servers, false, false);
        log.info("Test (2B): leader backs up quickly over incorrect follower logs");

        Random random = new Random();
        application.one(random.nextInt(100), servers, true);
        Thread.sleep(50);

        // 只留下一个 leader 和一个 follower
        int leader = application.checkOneLeader();
        application.disconnect((leader + 2) % servers);
        application.disconnect((leader + 3) % servers);
        application.disconnect((leader + 4) % servers);

        // 新增大量不会提交的日志（因为完成提交需要 leader 确认超过半数的 follower 成功复制）
        for (int i = 0; i < 50; i++) {
            application.getRaft(leader).startCmd(random.nextInt(100));
        }

        Thread.sleep(50);

        application.disconnect(leader);
        application.disconnect((leader + 1) % servers);

        // 恢复另一组 rafts
        application.connect((leader + 2) % servers);
        application.connect((leader + 3) % servers);
        application.connect((leader + 4) % servers);

        // 新增大量会提交的日志
        for (int i = 0; i < 50; i++) {
            application.one(random.nextInt(100), 3, true);
        }

        // 再次制造只有两个 raft 的场景
        int leader1 = application.checkOneLeader();
        int other = (leader + 2) % servers;
        if (leader1 == other) {
            other = (leader1 + 1) % servers;
        }
        application.disconnect(other);

        // 新增大量不会提交的日志（因为完成提交需要 leader 确认超过半数的 follower 成功复制）
        for (int i = 0; i < 50; i++) {
            application.getRaft(leader1).startCmd(random.nextInt(100));
        }

        Thread.sleep(50);

        // 恢复初代 leader，并附带两个 follower
        for (int i = 0; i < servers; i++) {
            application.disconnect(i);
        }
        application.connect(leader);
        application.connect((leader + 1) % servers);
        application.connect(other);

        // 新增大量会提交的日志
        for (int i = 0; i < 50; i++) {
            application.one(random.nextInt(100), 3, true);
        }

        // 连接所有 rafts
        for (int i = 0; i < servers; i++) {
            application.connect(i);
        }
        application.one(random.nextInt(100), servers, true);
        Object[] objects = application.nCommitted(102);

        Assertions.assertEquals(servers, objects[0]);

        application.cleanup();
    }

    @Test
    @Order(9)
    void testFigure82C() throws InterruptedException {
        int servers = 5;
        RaftApplication application = new RaftApplication();
        application.init(servers, false, false);
        log.info("Test (2B): leader backs up quickly over incorrect follower logs");

        Thread.sleep(50);
        Random random = new Random();
        application.one(random.nextInt(100), servers, true);

        // 模拟 raft 不断崩溃的情况下，重启持久化恢复是否成功以及日志的提交是否正常
        int count = servers;
        for (int i = 0; i < 100; i++) {
            int leader = -1;
            for (int j = 0; j < servers; j++) {
                if (application.getRaft(j) != null) {
                    int started = application.getRaft(j).startCmd(random.nextInt(100));
                    if (started != -1) {
                        leader = j;
                    }
                }
            }

            Thread.sleep(50);

            if (leader != -1) {
                application.crash(leader);
                count -= 1;
            }
            if (count < 3) {
                for (int j = 0; j < servers; j++) {
                    if (application.getRaft(j) == null && leader != j) {
                        application.startOne(j);
                        count += 1;
                        break;
                    }
                }
            }
            Thread.sleep(1000);
        }

        for (int i = 0; i < servers; i++) {
            application.startOne(i);
        }

        Thread.sleep(100);

        int index = application.one(random.nextInt(100), servers, true);
        Assertions.assertEquals(102, index);

        application.cleanup();
    }

    @Test
    @Order(10)
    void testReliableChurn2C() throws InterruptedException {
        int servers = 5;
        RaftApplication application = new RaftApplication();
        application.init(servers, false, false);
        log.info("Test (2C): churn");

        Thread.sleep(100);
        AtomicInteger stop = new AtomicInteger(0);
        Random random = new Random();

        // 创建三个并发线程不断写入命令，直到 stop 置 1
        int ncli = 3;
        Channel<List<Integer>> channel[] = new Channel[ncli];
        for (int i = 0; i < ncli; i++) {
            channel[i] = new Channel<>();
            final int me = i;
            final Channel<List<Integer>> chan = channel[i];
            // create concurrent clients
            new Thread(() -> {
                List<Integer> values = new ArrayList<>();
                while (stop.get() == 0) {
                    int x = random.nextInt(100);
                    int index = -1;
                    boolean ok = false;
                    for (int j = 0; j < servers; j++) {
                        application.getLock().lock();
                        Raft raft = application.getRaft(j);
                        application.getLock().unlock();
                        if (raft != null) {
                            int started = raft.startCmd(x);
                            if (started != -1) {
                                ok = true;
                                index = started;
                            }
                        }
                    }
                    if (ok) {
                        Long[] to = new Long[]{10L, 20L, 50L, 100L, 200L};
                        for (int j = 0; j < to.length; j++) {
                            Object[] objects = application.nCommitted(index);
                            if ((int) objects[0] > 0) {
                                if ((int) objects[1] == x) {
                                    values.add(x);
                                    break;
                                }
                            }
                            try {
                                Thread.sleep(to[j]);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    } else {
                        try {
                            Thread.sleep(79 + me * 17);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                chan.writeOne(values);
            }).start();
        }

        // 随机选择 disconnect，startOne，crash
        for (int iters = 0; iters < 20; iters++) {
            if (random.nextInt(Integer.MAX_VALUE) % 1000 < 200) {
                int i = random.nextInt(Integer.MAX_VALUE) % servers;
                application.disconnect(i);
            }

            if (random.nextInt(Integer.MAX_VALUE) % 1000 < 500) {
                int i = random.nextInt(Integer.MAX_VALUE) % servers;
                if (application.getRaft(i) == null) {
                    application.startOne(i);
                }
                application.connect(i);
            }

            if (random.nextInt(Integer.MAX_VALUE) % 1000 < 200) {
                int i = random.nextInt(Integer.MAX_VALUE) % servers;
                if (application.getRaft(i) != null) {
                    application.crash(i);
                }
            }

            Thread.sleep(200);
        }

        Thread.sleep(500);

        // 恢复所有 raft
        for (int i = 0; i < servers; i++) {
            if (application.getRaft(i) == null) {
                application.startOne(i);
            }
            application.connect(i);
        }

        stop.set(1);

        // 读取三个线程分别写入的指令集并合并到 values 中
        int[] values = new int[0];
        for (int i = 0; i < ncli; i++) {
            List<Integer> read = channel[i].readOne();
            if (CollectionUtil.isEmpty(read)) {
                log.error("client failed");
            }
            int n = values.length + read.size();
            int[] tmp = new int[n];
            System.arraycopy(values, 0, tmp, 0, values.length);
            for (int j = values.length; j < n; j++) {
                tmp[j] = read.get(j - values.length);
            }
            values = tmp;
        }

        Thread.sleep(500);

        int index = application.one(random.nextInt(100), servers, true);
        int[] really = new int[index - 1];
        for (int i = 0; i < index - 1; i++) {
            int cmd = (int) application.wait(i + 1, servers, -1);
            really[i] = cmd;
        }

        for (int x : values) {
            Assertions.assertTrue(Arrays.stream(really).anyMatch(y -> y == x));
        }

        application.cleanup();
    }

    int snapCommon(String name, boolean disconnect, boolean reliable, boolean crash) throws InterruptedException {
        int iters = 30;
        int servers = 3;
        RaftApplication application = new RaftApplication();
        application.init(servers, !reliable, true);
        log.info(name);

        Thread.sleep(50);
        Random random = new Random();
        application.one(random.nextInt(100), servers, true);
        int leader = application.checkOneLeader();

        for (int i = 0; i < iters; i++) {
            int victim = (leader + 1) % servers;
            int sender = leader;
            if (i % servers == 1) {
                sender = (leader + 1) % servers;
                victim = leader;
            }

            if (disconnect) {
                application.disconnect(victim);
                application.one(random.nextInt(100), servers - 1, true);
            }

            if (crash) {
                application.crash(victim);
                application.one(random.nextInt(100), servers - 1, true);
            }

            for (int j = 0; j < SNAPSHOT_INTERVAL + 1; j++) {
                int cmd = random.nextInt(100);
                Raft raft = application.getRaft(sender);
                if (raft != null) {
                    application.getRaft(sender).startCmd(cmd);
                }
            }
            application.one(random.nextInt(100), servers - 1, true);

            if (application.logByteSize() >= MAX_LOG_SIZE) {
                log.error("log size too large");
            }

            if (disconnect) {
                application.connect(victim);
                application.one(random.nextInt(100), servers, true);
                leader = application.checkOneLeader();
            }

            if (crash) {
                application.startOne(victim);
                application.connect(victim);
                application.one(random.nextInt(100), servers, true);
                leader = application.checkOneLeader();
            }
        }

        application.cleanup();
        return application.logSize();
    }

    @Test
    @Order(11)
    void testSnapshotBasic2D() throws InterruptedException {
        int cnt = snapCommon("Test (2D): snapshots basic", false, true, false);
        Assertions.assertEquals(251, cnt);
    }

    @Test
    @Order(12)
    void testSnapshotInstall2D() throws InterruptedException {
        snapCommon("Test (2D): install snapshots (disconnect)", true, true, false);
    }

    @Test
    @Order(13)
    void testSnapshotInstallCrash2D() throws InterruptedException {
        snapCommon("Test (2D): install snapshots (crash)", false, true, true);
    }

}
