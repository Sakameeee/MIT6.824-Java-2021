package com.sakame.model.dto;

import lombok.Data;

import java.io.Serializable;

/**
 * @author sakame
 * @version 1.0
 */
@Data
public class CommandResponse implements Serializable {

    /**
     * 成功标识
     */
    private boolean success;

    /**
     * 错误标识
     */
    private String err;

    /**
     * 结果值
     */
    private String value;

}
