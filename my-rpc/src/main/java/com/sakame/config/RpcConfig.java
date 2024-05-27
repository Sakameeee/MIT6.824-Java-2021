package com.sakame.config;

import com.sakame.constant.RpcConstant;
import com.sakame.registry.Registry;
import com.sakame.registry.RegistryFactory;
import com.sakame.serializer.SerializerKeys;
import com.sakame.utils.ConfigUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * rpc 配置框架
 *
 * @author sakame
 * @version 1.0
 */
@Data
@Slf4j
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RpcConfig {
    /**
     * 单例
     */
    private static volatile RpcConfig rpcConfig;
    /**
     * 名称
     */
    private String name = "my-rpc";
    /**
     * 版本号
     */
    private String version = "1.0";
    /**
     * 主机名
     */
    private String serverHost = "localhost";
    /**
     * 端口号
     */
    private Integer serverPort = 8081;
    /**
     * 序列化器
     */
    private String serializer = SerializerKeys.JDK;
    /**
     * 模拟调用
     */
    private boolean mock = false;
    /**
     * 注册中心配置
     */
    private RegistryConfig registryConfig = new RegistryConfig();

    /**
     * 传入配置初始化
     *
     * @param newRpcConfig
     */
    public static void init(RpcConfig newRpcConfig) {
        rpcConfig.setName(newRpcConfig.getName());
        rpcConfig.setVersion(newRpcConfig.getVersion());
        rpcConfig.setServerHost(newRpcConfig.getServerHost());
        rpcConfig.setServerPort(newRpcConfig.getServerPort());
        rpcConfig.setMock(newRpcConfig.isMock());
        rpcConfig.setRegistryConfig(newRpcConfig.getRegistryConfig());
        rpcConfig.setSerializer(newRpcConfig.getSerializer());
        log.info("rpc init, config = {}", rpcConfig);

        RegistryConfig registryConfig = rpcConfig.getRegistryConfig();
        Registry registry = RegistryFactory.getInstance(registryConfig.getRegistry());
        registry.init(registryConfig);
        log.info("registry init, config = {}", registryConfig);

        Runtime.getRuntime().addShutdownHook(new Thread(registry::destroy));
    }

    /**
     * 获取配置
     *
     * @return
     */
    public static RpcConfig getRpcConfig() {
        if (rpcConfig == null) {
            synchronized (RpcConfig.class) {
                if (rpcConfig == null) {
                    rpcConfig = ConfigUtils.loadConfig(RpcConfig.class, RpcConstant.DEFAULT_CONFIG_PREFIX);
                    init(rpcConfig);
                }
            }
        }
        return rpcConfig;
    }
}
