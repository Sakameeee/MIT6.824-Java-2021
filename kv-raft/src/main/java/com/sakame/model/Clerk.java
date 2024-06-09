package com.sakame.model;

import com.sakame.service.KVServerService;
import lombok.Data;

/**
 * @author sakame
 * @version 1.0
 */
@Data
public class Clerk {

    /**
     * KVServer 的服务调用数组
     */
    private KVServerService[] services;

    /**
     * 记录当前的 leader 位置
     */
    private int leaderId;

    /**
     * client 标识
     */
    private int clientId;

    /**
     * 操作标识
     */
    private int seqId;

}
