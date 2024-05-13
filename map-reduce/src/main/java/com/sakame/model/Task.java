package com.sakame.model;

import lombok.Data;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Timer;

/**
 * 任务类
 * @author sakame
 * @version 1.0
 */
@Data
public class Task {
    /**
     * 任务 id
     */
    private int taskId;

    /**
     * 文件列表
     */
    private List<String> files;

    /**
     * 任务状态
     */
    private int status;

    /**
     * 任务开始时间
     */
    private LocalDateTime startTime;
}
