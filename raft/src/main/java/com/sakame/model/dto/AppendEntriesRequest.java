package com.sakame.model.dto;

import com.sakame.model.Entry;
import lombok.Data;

import java.io.Serializable;

/**
 * @author sakame
 * @version 1.0
 */
@Data
public class AppendEntriesRequest implements Serializable {

    /**
     * leader term
     */
    private int term;

    /**
     * leader id
     */
    private int leaderId;

    /**
     * 用于 raft 检查对应位置的日志与自身日志是否冲突
     * 对应的是日志索引，使用前要转换成数组对应的索引
     */
    private int preLogIndex = 0;

    /**
     * 用于检查 term 是否一致
     */
    private int preLogTerm = -1;

    /**
     * 复制了 preLogIndex 到 leader 最新日志的区间，用于日志追赶
     * 第一次发送为空
     */
    private Entry[] entries;

    /**
     * leader 最近提交索引位置
     */
    private int leaderCommit;

}
