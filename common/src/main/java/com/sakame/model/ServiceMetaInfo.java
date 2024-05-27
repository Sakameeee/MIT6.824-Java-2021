package com.sakame.model;

import cn.hutool.core.util.StrUtil;
import lombok.Data;

/**
 * 服务元信息
 *
 * @author sakame
 * @version 1.0
 */
@Data
public class ServiceMetaInfo {

    /**
     * 服务名称
     */
    private String serviceName;

    /**
     * 实现类
     */
    private String implClass;

    /**
     * 服务版本号
     */
    private String serviceVersion;

    /**
     * 服务域名
     */
    private String serviceHost;

    /**
     * 服务端口号
     */
    private Integer servicePort;

    /**
     * 服务分组
     */
    private String serviceGroup = "default";

    /**
     * 获取服务键名
     *
     * @return
     */
    public String getServiceKey() {
        return String.format("%s:%s", serviceName, serviceVersion);
    }

    /**
     * 获取服务注册节点键名
     *
     * @return
     */
    public String getServiceNodeKey() {
        return String.format("%s/%s:%s", getServiceKey(), serviceHost, servicePort);
    }

    public String getServiceAddress() {
        if (!StrUtil.contains(serviceHost, "http")) {
            return String.format("http://%s:%s", serviceHost, servicePort);
        }
        return String.format("%s:%s", serviceHost, servicePort);
    }

}
