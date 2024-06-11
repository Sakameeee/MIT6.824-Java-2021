package com.sakame;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.clone.CloneSupport;
import cn.hutool.core.util.PrimitiveArrayUtil;
import com.sakame.model.RaftState;
import com.sakame.model.RaftStatePersist;
import com.sakame.serializer.Serializer;
import com.sakame.serializer.SerializerFactory;
import com.sakame.serializer.SerializerKeys;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 持久化对象，内置对 raft state 持久化的函数，snapshot 用于定制数据持久化
 *
 * @author sakame
 * @version 1.0
 */
public class Persister extends CloneSupport<Persister> {

    private final ReentrantLock lock = new ReentrantLock();

    private static final Serializer serializer = SerializerFactory.getInstance(SerializerKeys.KRYO);

    /**
     * 存放 raft state 的部分属性
     */
    private byte[] raftState = new byte[0];

    /**
     * 存放裁切位置对应的 cmd
     */
    private byte[] snapshot = new byte[0];

    /**
     * 持久化 raft state
     *
     * @param raftState
     */
    public void persist(RaftState raftState) {
        saveRaftState(genRaftStateBytes(raftState));
    }

    /**
     * 持久化 raft state 和存储 snapshot 数据
     *
     * @param raftState
     * @param snapshot
     */
    public void persistSnapshot(RaftState raftState, byte[] snapshot) {
        saveRaftStateAndSnapshot(genRaftStateBytes(raftState), snapshot);
    }

    /**
     * 恢复 raft state 数据，不操作 snapshot
     *
     * @param raftState
     */
    public void readPersist(RaftState raftState) {
        byte[] source = readRaftState();
        if (PrimitiveArrayUtil.isEmpty(source) || source.length == 1) {
            return;
        }

        try {
            RaftStatePersist deserialize = serializer.deserialize(source, RaftStatePersist.class);
            BeanUtil.copyProperties(deserialize, raftState);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据 raftState 对象生成 byte 数组数据
     *
     * @param raftState
     * @return
     */
    public byte[] genRaftStateBytes(RaftState raftState) {
        RaftStatePersist raftStatePersist = new RaftStatePersist();
        BeanUtil.copyProperties(raftState, raftStatePersist);
        try {
            return serializer.serialize(raftStatePersist);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] copy(byte[] source) {
        if (PrimitiveArrayUtil.isEmpty(source)) {
            return new byte[0];
        }
        return Arrays.copyOf(source, source.length);
    }

    public void saveRaftState(byte[] source) {
        lock.lock();
        raftState = source;
        lock.unlock();
    }

    public byte[] readRaftState() {
        lock.lock();
        byte[] bytes = copy(raftState);
        lock.unlock();
        return bytes;
    }

    public int raftStateSize() {
        lock.lock();
        int size = raftState.length;
        lock.unlock();
        return size;
    }

    public void saveSnapshot(byte[] source) {
        lock.lock();
        snapshot = source;
        lock.unlock();
    }

    public byte[] readSnapshot() {
        lock.lock();
        byte[] bytes = copy(snapshot);
        lock.unlock();
        return bytes;
    }

    public int snapshotSize() {
        lock.lock();
        int size = snapshot.length;
        lock.unlock();
        return size;
    }

    public void saveRaftStateAndSnapshot(byte[] raftState, byte[] snapshot) {
        lock.lock();
        this.raftState = raftState;
        this.snapshot = snapshot;
        lock.unlock();
    }

}
