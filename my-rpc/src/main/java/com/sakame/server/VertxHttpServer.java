package com.sakame.server;

import com.sakame.server.handler.HttpServerHandler;
import io.vertx.core.Vertx;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.checkerframework.checker.units.qual.N;

/**
 * @author sakame
 * @version 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class VertxHttpServer implements HttpServer{

    private Object instance;

    @Override
    public void doStart(int port) {
        Vertx vertx = Vertx.vertx();

        io.vertx.core.http.HttpServer httpServer = vertx.createHttpServer();

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
}
