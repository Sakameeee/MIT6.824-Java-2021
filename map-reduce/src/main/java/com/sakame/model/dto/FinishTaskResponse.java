package com.sakame.model.dto;

import lombok.Data;

import java.io.Serializable;

/**
 * @author sakame
 * @version 1.0
 */
@Data
public class FinishTaskResponse implements Serializable {
    /**
     * 是否成功接收
     */
    private boolean isOK;
}
