package com.sakame.model;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author sakame
 * @version 1.0
 */
public class Channel<T> {

    private static final ReentrantLock lock = new ReentrantLock();

    private Condition condition = lock.newCondition();

    private T object;

}
