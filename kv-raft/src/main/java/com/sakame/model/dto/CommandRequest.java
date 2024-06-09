package com.sakame.model.dto;

import com.sakame.model.Command;
import lombok.Data;

import java.io.Serializable;

/**
 * @author sakame
 * @version 1.0
 */
@Data
public class CommandRequest implements Serializable {

    /**
     * 命令
     */
    private Command command;

}
