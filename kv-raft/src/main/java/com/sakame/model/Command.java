package com.sakame.model;

import com.sakame.common.CommandType;
import lombok.Data;

import java.io.Serializable;

/**
 * @author sakame
 * @version 1.0
 */
@Data
public class Command implements Serializable {

    /**
     * 命令类型
     */
    private CommandType type;

    /**
     * 键值
     */
    private String key;

    /**
     * 值
     */
    private String value;

    /**
     * 客户端 id
     */
    private int clientId;

    /**
     * 序列 id
     */
    private int seqId;

}
