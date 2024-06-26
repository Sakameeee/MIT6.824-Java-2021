package com.sakame.model.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author sakame
 * @version 1.0
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class FinishTaskRequest implements Serializable {
    /**
     * worker id
     */
    private String workerId;

    /**
     * 任务 id
     */
    private int taskId;

    /**
     * 任务类型
     */
    private int type;
}
