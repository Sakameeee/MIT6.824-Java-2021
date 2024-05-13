package com.sakame.model;

import com.sakame.constant.MasterStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 用于状态管理
 * @author sakame
 * @version 1.0
 */
@Data
@NoArgsConstructor
public class CoordinatorStatus {
    /**
     * 文件列表
     */
    private List<String> files;

    /**
     * 切片个数
     */
    private int nReduce;

    /**
     * 任务列表
     */
    private List<Task> tasks;

    /**
     * 任务个数
     */
    private int mapCount;

    /**
     * 已完成提交的 worker map
     */
    private Map<String, Integer> workerCommit;

    /**
     * 记录是否所有任务都已提交
     */
    private boolean allCommitted;

    /**
     * 超时时间
     */
    private int timeout;

    /**
     * coordinator 的状态
     */
    private int status;

    /**
     * 可重入锁
     */
    private ReentrantLock reentrantLock;
}
