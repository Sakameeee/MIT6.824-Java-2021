package com.sakame.proxy;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * 模拟调用
 * @author sakame
 * @version 1.0
 */
@Slf4j
public class MockServiceProxy implements InvocationHandler {
    @Override
    public Object invoke(Object o, Method method, Object[] objects) throws Throwable {
        Class<?> returnType = method.getReturnType();
        log.info("mock invoke:{}", returnType);
        return getDefaultObject(returnType);
    }

    public Object getDefaultObject(Class<?> c) {
        if (c.isPrimitive()) {
            if (c == short.class) {
                return 0;
            } else if(c == boolean.class) {
                return true;
            } else if (c == long.class) {
                return 0L;
            } else if (c == int.class){
                return 0;
            }
        }
        return null;
    }
}
