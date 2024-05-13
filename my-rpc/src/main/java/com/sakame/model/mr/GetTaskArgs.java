package com.sakame.model.mr;

import lombok.Data;

import java.io.Serializable;

/**
 * @author sakame
 * @version 1.0
 */
@Data
public class GetTaskArgs implements Serializable {
    /**
     * worker 的唯一标识
     */
    private String workerId;
}
