package com.sakame.server;

/**
 * @author sakame
 * @version 1.0
 */
public interface HttpServer {
    /**
     * 启动一个 http 服务器
     * @param port
     */
    void doStart(int port);

    /**
     * 关闭服务器
     */
    void doShutdown();
}
