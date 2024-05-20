package com.sakame.model.dto;

import lombok.Data;

import java.io.Serializable;

/**
 * @author sakame
 * @version 1.0
 */
@Data
public class ApplyMsg implements Serializable {

    /**
     * 消息是否包含追加日志内容
     */
    private boolean commandValid;

    /**
     * 具体的日志操作命令
     */
    private Object command;

    /**
     * 指令索引
     */
    private int commandIndex;

    private boolean snapShotValid;

    private byte[] snapShot;

    private int snapShotTerm;

    private int snapShotIndex;

}
