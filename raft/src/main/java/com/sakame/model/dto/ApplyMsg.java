package com.sakame.model.dto;

import lombok.Data;

import java.io.Serializable;

/**
 * 用于 application 从 raft 读取信息
 *
 * @author sakame
 * @version 1.0
 */
@Data
public class ApplyMsg implements Serializable {

    /**
     * 消息是否包含追加日志内容
     */
    private boolean commandValid = false;

    /**
     * 具体的日志操作命令
     */
    private Object command;

    /**
     * 指令索引
     */
    private int commandIndex;

    /**
     * 指令索引对应的 term
     */
    private int commandTerm;

    /**
     * 消息是否包含快照内容
     */
    private boolean snapShotValid = false;

    /**
     * 快照数据
     */
    private byte[] snapShot;

    /**
     * 快照裁切索引位置对应的 term
     */
    private int snapShotTerm;

    /**
     * 快照裁切索引位置
     */
    private int snapShotIndex;

}
