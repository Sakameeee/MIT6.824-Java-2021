package com.sakame.provider;

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
        LocalRegistry.register(UserService.class.getName(), UserServiceImpl.class);

        HttpServer httpServer = new VertxHttpServer();
        httpServer.doStart(8080);
    }
}
