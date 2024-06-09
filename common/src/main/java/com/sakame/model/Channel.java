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

    private int capacity = -1;

    public Channel() {
    }

    public Channel(int capacity) {
        queue = new LinkedBlockingQueue<>(capacity);
        this.capacity = capacity;
    }

    public boolean writeOne(T o) {
        if (capacity != -1) {
            return write(o);
        }
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
        return true;
    }

    public T readOne() {
        if (capacity != -1) {
            return read();
        }
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

    public boolean write(T o) {
        if (capacity == -1) {
            return writeOne(o);
        }
        boolean ret;
        try {
            ret = queue.offer(o, 100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    public T read() {
        if (capacity == -1) {
            return readOne();
        }
        try {
            return queue.poll(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
