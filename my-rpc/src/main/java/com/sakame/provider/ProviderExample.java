package com.sakame.provider;

import com.sakame.config.RegistryConfig;
import com.sakame.config.RpcConfig;
import com.sakame.model.ServiceMetaInfo;
import com.sakame.registry.LocalRegistry;
import com.sakame.registry.Registry;
import com.sakame.registry.RegistryFactory;
import com.sakame.serializer.SerializerKeys;
import com.sakame.server.HttpServer;
import com.sakame.server.VertxHttpServer;
import com.sakame.service.UserService;

/**
 * @author sakame
 * @version 1.0
 */
public class ProviderExample {
    public static void main(String[] args) {
        RpcConfig rpcConfig = RpcConfig.getRpcConfig();

        String serviceName = UserService.class.getName();

        RegistryConfig registryConfig = rpcConfig.getRegistryConfig();
        Registry registry = RegistryFactory.getInstance(registryConfig.getRegistry());
        ServiceMetaInfo serviceMetaInfo = new ServiceMetaInfo();
        serviceMetaInfo.setServiceName(serviceName);
        serviceMetaInfo.setServiceVersion("1.0");
        serviceMetaInfo.setServiceHost(rpcConfig.getServerHost());
        serviceMetaInfo.setServicePort(rpcConfig.getServerPort());
        try {
            registry.register(serviceMetaInfo);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        LocalRegistry.register(serviceName, UserServiceImpl.class);

        // http 服务器的端口号和服务注册的端口号一致
        HttpServer httpServer = new VertxHttpServer();
        httpServer.doStart(rpcConfig.getServerPort());
    }
}
