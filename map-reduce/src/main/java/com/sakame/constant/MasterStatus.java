package com.sakame.constant;

/**
 * coordinator 常量
 * @author sakame
 * @version 1.0
 */
public interface MasterStatus {
    /**
     * map 任务阶段
     */
    int MAP_PHASE = 0;

    /**
     * reduce 任务阶段
     */
    int REDUCE_PHASE = 1;

    /**
     * 已完成
     */
    int FINISH_PHASE = 2;
}
