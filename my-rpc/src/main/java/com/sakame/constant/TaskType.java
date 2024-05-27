package com.sakame.constant;

/**
 * 任务类型
 *
 * @author sakame
 * @version 1.0
 */
public interface TaskType {
    /**
     * map 任务
     */
    int MAP = 0;

    /**
     * reduce 任务
     */
    int REDUCE = 1;

    /**
     * 等待中
     */
    int WAIT = 2;

    /**
     * 中断
     */
    int STOP = 3;
}
