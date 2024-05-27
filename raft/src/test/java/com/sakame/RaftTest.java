package com.sakame;

import cn.hutool.core.util.ArrayUtil;
import com.sakame.model.Channel;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

/**
 * @author sakame
 * @version 1.0
 */
@Slf4j
class RaftTest {

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

        application.cleanup();
        Assertions.assertNotEquals(-1, leader1);
        Assertions.assertEquals(leader2, leader3);
        Assertions.assertNotEquals(-1, leader4);
        Assertions.assertEquals(leader4, leader5);
    }

    @Test
    @Order(3)
    void TestBasicAgree2B() throws InterruptedException {
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

        Thread.sleep(50);
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

}
