package com.sakame.provider;

import com.sakame.config.RpcConfig;
import com.sakame.registry.LocalRegistry;
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
        System.out.println(rpcConfig);
        RpcConfig.init(RpcConfig.builder().serverPort(8081).build());
        System.out.println(rpcConfig);

        LocalRegistry.register(UserService.class.getName(), UserServiceImpl.class);

        HttpServer httpServer = new VertxHttpServer();
        httpServer.doStart(rpcConfig.getServerPort());
    }
}
