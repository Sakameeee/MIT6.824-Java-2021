package com.sakame.registry;

import com.sakame.config.RegistryConfig;
import com.sakame.model.ServiceMetaInfo;

import java.util.List;

/**
 * 注册中心
 * @author sakame
 * @version 1.0
 */
public interface Registry {

    /**
     * 服务中心初始化
     * @param registryConfig
     */
    void init(RegistryConfig registryConfig);

    /**
     * 服务注册（服务端）
     * @param serviceMetaInfo
     * @throws Exception
     */
    void register(ServiceMetaInfo serviceMetaInfo) throws Exception;

    /**
     * 注销服务（服务端）
     * @param serviceMetaInfo
     */
    void unRegister(ServiceMetaInfo serviceMetaInfo);

    /**
     * 发现某服务的所有节点（消费端）
     * @param serviceKey
     * @return
     */
    List<ServiceMetaInfo> serviceDiscovery(String serviceKey);

    /**
     * 服务销毁
     */
    void destroy();

    /**
     * 心跳机制（服务端）
     */
    void heartBeat();

    /**
     * 服务监听（消费端）
     * @param serviceNodeKey
     */
    void watch(String serviceNodeKey);

}
