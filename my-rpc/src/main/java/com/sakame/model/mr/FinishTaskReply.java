package com.sakame.model.mr;

import lombok.Data;

import java.io.Serializable;

/**
 * @author sakame
 * @version 1.0
 */
@Data
public class FinishTaskReply implements Serializable {
    /**
     * 是否成功接收
     */
    private boolean isOK;
}
