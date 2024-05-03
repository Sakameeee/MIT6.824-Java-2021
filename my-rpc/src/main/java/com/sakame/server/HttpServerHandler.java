package com.sakame.server;

import com.sakame.model.RpcRequest;
import com.sakame.model.RpcResponse;
import com.sakame.registry.LocalRegistry;
import com.sakame.serializer.JdkSerializer;
import com.sakame.serializer.Serializer;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * @author sakame
 * @version 1.0
 */
public class HttpServerHandler implements Handler<HttpServerRequest> {
    @Override
    public void handle(HttpServerRequest httpServerRequest) {
        // 指定序列化器
        final Serializer serializer = new JdkSerializer();

        // 记录日志
        System.out.println("Received request:" + httpServerRequest.method() + " " + httpServerRequest.uri());

        // 异步处理 http 请求
        httpServerRequest.bodyHandler(body -> {
            byte[] bytes = body.getBytes();
            // 反序列化获得 request
            RpcRequest rpcRequest = null;
            try {
                rpcRequest = serializer.deserialize(bytes, RpcRequest.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            RpcResponse rpcResponse = new RpcResponse();
            if (rpcResponse == null) {
                rpcResponse.setMessage("rpcRequest is null");
                doResponse(httpServerRequest, rpcResponse, serializer);
                return;
            }

            // 反射调用服务类并返回响应
            try {
                Class<?> implClass = LocalRegistry.get(rpcRequest.getServiceName());
                Method method = implClass.getMethod(rpcRequest.getMethodName(), rpcRequest.getParameterTypes());
                Object result = method.invoke(implClass.newInstance(), rpcRequest.getArgs());
                rpcResponse.setData(result);
                rpcResponse.setMessage("ok");
                rpcResponse.setDataType(method.getReturnType());
            } catch (Exception e) {
                e.printStackTrace();
                rpcResponse.setMessage(e.getMessage());
                rpcResponse.setException(e);
            }

            doResponse(httpServerRequest, rpcResponse, serializer);
        });
    }

    /**
     * 响应方法，把响应体序列化返回
     * @param request
     * @param rpcResponse
     * @param serializer
     */
    void doResponse(HttpServerRequest request, RpcResponse rpcResponse, Serializer serializer) {
        HttpServerResponse httpServerResponse = request.response()
                .putHeader("content-type", "application/json");
        try {
            byte[] serializered = serializer.serialize(rpcResponse);
            httpServerResponse.end(Buffer.buffer(serializered));
        } catch (IOException e) {
            e.printStackTrace();
            httpServerResponse.end(Buffer.buffer());
        }
    }
}