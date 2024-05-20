package com.sakame;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author sakame
 * @version 1.0
 */
@Slf4j
public class RaftTest {

    @Test
    public void testInitialElection() throws InterruptedException {
        int servers = 3;
        RaftApplication application = new RaftApplication();
        application.init(servers, false, false);
        log.info("Test (2A): initial election");

        Thread.sleep(1000);
        int leader1 = application.checkOneLeader();

        Thread.sleep(50);
        int term1 = application.checkTerms();

        Thread.sleep(600);
        int term2 = application.checkTerms();

        int leader2 = application.checkOneLeader();

        Assertions.assertNotEquals(-1, leader2);
        Assertions.assertEquals(leader1, leader2);
        Assertions.assertEquals(term1, term2);
    }

    @Test
    public void testReElection() throws InterruptedException {
        int servers = 3;
        RaftApplication application = new RaftApplication();
        application.init(servers, false, false);
        log.info("Test (2A): election after network failure");

        Thread.sleep(1000);
        int leader1 = application.checkOneLeader();

        application.disconnect(leader1);
        Thread.sleep(1000);

        int leader2 = application.checkOneLeader();
        application.connect(leader1);
        int leader3 = application.checkOneLeader();

        application.disconnect(leader2);
        application.disconnect((leader2 + 1) % servers);
        Thread.sleep(1000);
        application.checkNoLeader();

        application.connect((leader2 + 1) % servers);
        int leader4 = application.checkOneLeader();

        application.connect(leader2);
        int leader5 = application.checkOneLeader();

        Assertions.assertNotEquals(-1, leader1);
        Assertions.assertEquals(leader2, leader3);
        Assertions.assertNotEquals(-1, leader4);
        Assertions.assertEquals(leader4, leader5);
    }
}
