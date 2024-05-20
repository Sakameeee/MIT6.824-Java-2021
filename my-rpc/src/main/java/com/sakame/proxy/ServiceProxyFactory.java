package com.sakame.proxy;

import com.sakame.config.RpcConfig;

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
        if (RpcConfig.getRpcConfig().isMock()) {
            return getMockProxy(serviceClass);
        }

        return (T) Proxy.newProxyInstance(
                serviceClass.getClassLoader(),
                new Class[]{serviceClass},
                new ServiceProxy()
        );
    }

    public static <T> T getProxy(Class<T> serviceClass, boolean callAll, int except) {
        return (T) Proxy.newProxyInstance(
                serviceClass.getClassLoader(),
                new Class[]{serviceClass},
                new ServiceProxy(callAll, except)
        );
    }

    public static <T> T getProxy(Class<T> serviceClass, int chosen) {
        return (T) Proxy.newProxyInstance(
                serviceClass.getClassLoader(),
                new Class[]{serviceClass},
                new ServiceProxy(chosen)
        );
    }

    public static <T> T getMockProxy(Class<T> serviceClass) {
        return (T) Proxy.newProxyInstance(
                serviceClass.getClassLoader(),
                new Class[]{serviceClass},
                new MockServiceProxy()
        );
    }
}
