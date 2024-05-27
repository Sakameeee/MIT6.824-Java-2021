package com.sakame.server;

import com.sakame.server.handler.HttpServerHandler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author sakame
 * @version 1.0
 */
@Data
@NoArgsConstructor
public class VertxHttpServer implements HttpServer {

    private Object instance;

    private io.vertx.core.http.HttpServer httpServer;

    public VertxHttpServer(Object o) {
        instance = o;
    }

    @Override
    public void doStart(int port) {
        Vertx vertx = Vertx.vertx();

        httpServer = vertx.createHttpServer(new HttpServerOptions());

        if (instance == null) {
            httpServer.requestHandler(new HttpServerHandler());
        } else {
            httpServer.requestHandler(new HttpServerHandler(instance));
        }

        httpServer.listen(port, result -> {
            if (result.succeeded()) {
                System.out.println("Server is now listening port:" + port);
            } else {
                System.out.println("Failed to start server:" + result.cause());
            }
        });
    }

    @Override
    public void doShutdown() {
        if (httpServer == null) {
            return;
        }
        httpServer.close();
    }
}
