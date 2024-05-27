package com.sakame.model;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 定制双线程通信，读取和写入交替进行
 *
 * @author sakame
 * @version 1.0
 */
public class Channel<T> {

    private final ReentrantLock lock = new ReentrantLock();

    private final Condition writeCond = lock.newCondition();

    private final Condition readCond = lock.newCondition();

    private T object;

    private BlockingQueue<T> queue;

    public Channel() {
    }

    public Channel(int capacity) {
        queue = new LinkedBlockingQueue<>(capacity);

    }

    public void writeOne(T o) {
        lock.lock();

        try {
            while (object != null) {
                writeCond.await();
            }

            object = o;
            readCond.signal();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    public T readOne() {
        T tmp = null;
        lock.lock();

        try {
            while (object == null) {
                readCond.await();
            }

            tmp = object;
            object = null;
            writeCond.signal();
            return tmp;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    public void write(T o) {
        try {
            queue.offer(o, 100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public T read() {
        try {
            return queue.poll(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
