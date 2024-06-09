package com.sakame.service;

import com.sakame.model.dto.CommandRequest;
import com.sakame.model.dto.CommandResponse;

/**
 * @author sakame
 * @version 1.0
 */
public interface KVServerService {

    /**
     * 响应来自 client 的命令
     *
     * @param request 请求体
     * @return 响应体
     */
    CommandResponse requestCommand(CommandRequest request);

}
