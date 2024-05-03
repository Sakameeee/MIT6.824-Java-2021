package com.sakame.proxy;

import java.lang.reflect.Proxy;

/**
 * 服务代理工厂
 * @author sakame
 * @version 1.0
 */
public class ServiceProxyFactory {
    /**
     * 获取代理对象
     * @param serviceClass
     * @return
     * @param <T>
     */
    public static <T> T getProxy(Class<T> serviceClass) {
        return (T) Proxy.newProxyInstance(
                serviceClass.getClassLoader(),
                new Class[]{serviceClass},
                new ServiceProxy()
        );
    }
}
