package com.sakame.server;

import io.vertx.core.Vertx;

/**
 * @author sakame
 * @version 1.0
 */
public class VertxHttpServer implements HttpServer{
    @Override
    public void doStart(int port) {
        Vertx vertx = Vertx.vertx();

        io.vertx.core.http.HttpServer httpServer = vertx.createHttpServer();

        httpServer.requestHandler(new HttpServerHandler());

        httpServer.listen(port, result -> {
            if (result.succeeded()) {
                System.out.println("Server is now listening port:" + port);
            } else {
                System.out.println("Failed to start server:" + result.cause());
            }
        });
    }
}
