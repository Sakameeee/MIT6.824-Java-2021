package com.sakame.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author sakame
 * @version 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Entry implements Serializable {

    /**
     * 条目索引
     */
    private int index = 0;

    /**
     * 条目对应的 term
     */
    private int term = -1;

    /**
     * 条目对应的命令
     */
    private Object command;

}
