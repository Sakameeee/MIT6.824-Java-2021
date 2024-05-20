package com.sakame.model.dto;

import lombok.Data;

import java.io.Serializable;

/**
 * 投票请求
 * @author sakame
 * @version 1.0
 */
@Data
public class RequestVoteRequest implements Serializable {

    /**
     * 发送方的 term
     */
    private int term;

    /**
     * 候选者 id
     */
    private int candidateId;

    /**
     * 发送方日志的最后一条信息对应的 term
     */
    private int lastLogTerm;

    /**
     * 发送方日志的最后一条消息对应的索引
     */
    private int lastLogIndex;

}
