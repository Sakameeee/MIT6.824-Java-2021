package com.sakame.common;

import com.sakame.model.dto.CommandResponse;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 命令上下文，KVServer 记录成功提交的命令
 *
 * @author sakame
 * @version 1.0
 */
@Data
@AllArgsConstructor
public class CommandContext {

    /**
     * 序列 id，client 每发送一个命令 +1
     */
    private int seqId;

    /**
     * id 对应的 client 发送的命令的 response
     */
    private CommandResponse response;

}
