package com.sakame.model;

import lombok.Data;

/**
 * @author sakame
 * @version 1.0
 */
@Data
public class Entry {

    /**
     * 条目索引
     */
    private int index = 0;

    /**
     * 条目对应的 term
     */
    private int term = 0;

    /**
     * 条目对应的命令
     */
    private Object command;

}
