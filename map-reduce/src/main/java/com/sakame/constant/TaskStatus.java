package com.sakame.constant;

/**
 * 定义任务状态
 *
 * @author sakame
 * @version 1.0
 */
public interface TaskStatus {
    /**
     * 空闲
     */
    int IDLE = 0;

    /**
     * 正在处理
     */
    int IN_PROGRESS = 1;

    /**
     * 已完成
     */
    int COMPLETED = 2;
}
