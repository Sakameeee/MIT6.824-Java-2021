package com.sakame.model.dto;

import lombok.Data;

import java.io.Serializable;

/**
 * @author sakame
 * @version 1.0
 */
@Data
public class AppendEntriesResponse implements Serializable {

    /**
     * 接收方的 term
     */
    private int term;

    /**
     * 复制是否成功
     */
    private boolean succeeded;

    /**
     * 复制不成功则修改该值
     * 告知 leader 最靠后的冲突日志的索引
     */
    private int conflictIndex;

    /**
     * 冲突日志对应的 term
     */
    private int conflictTerm;

}
