package com.sakame.utils;

import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Rpc 工具类
 *
 * @author sakame
 * @version 1.0
 */
@Slf4j
public class RpcUtils {

    private static class Pair {
        private int first;
        private int second;

        Pair(int first, int second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            Pair pair = (Pair) obj;
            return pair.first == this.first && pair.second == this.second;
        }

        @Override
        public int hashCode() {
            int result = Integer.hashCode(first);
            result = 31 * result + Integer.hashCode(second);
            return result;
        }
    }

    private static final Map<String, Set<Pair>> net = new HashMap<>();

    /**
     * 反射代理调用，通过 map 鉴权
     * 捕获异常并返回 null 不影响程序正常运行，可能的异常：service 为 null，服务器关闭
     *
     * @param from       调用者
     * @param to         被调用者
     * @param instance   service 实例
     * @param methodName 调用的服务名称
     * @param args       服务参数
     * @param <T>
     * @return 响应体
     */
    @SuppressWarnings("unchecked")
    public static <T> T call(int from, int to, Object instance, String methodName, Object... args) {
        Object invoke = null;
        try {
            // 获取对象的类类型
            Class<?> clazz = instance.getClass();
            String key = "";
            Class<?>[] interfaces = clazz.getInterfaces();
            for (Class<?> i : interfaces) {
                if (StrUtil.contains(i.getName(), "Service")) {
                    key = i.getName();
                    break;
                }
            }
            if (net.containsKey(key) && net.get(key).contains(new Pair(from, to))) {
                return null;
            }

            // 获取方法参数类型
            Class<?>[] parameterTypes = new Class[args.length];
            for (int i = 0; i < args.length; i++) {
                if (args[i] instanceof Integer) {
                    parameterTypes[i] = int.class; // 特殊处理基本数据类型
                } else if (args[i] instanceof Boolean) {
                    parameterTypes[i] = boolean.class;
                } else {
                    parameterTypes[i] = args[i] == null ? Object.class : args[i].getClass();
                }
            }

            // 获取方法对象
            Method method = clazz.getDeclaredMethod(methodName, parameterTypes);
            method.setAccessible(true);

            // 调用方法并返回结果
            invoke = method.invoke(instance, args);
        } catch (Exception e) {
            log.debug(e.getMessage());
        }
        return (T) invoke;
    }

    /**
     * 启用 from 到 to 的调用服务权限
     *
     * @param clazz 服务类别
     * @param from  调用者
     * @param to    被调用者
     */
    public static void enable(Class<?> clazz, int from, int to) {
        String name = clazz.getName();
        if (!net.containsKey(name)) {
            net.put(name, new HashSet<>());
            return;
        }
        net.get(name).remove(new Pair(from, to));
    }

    /**
     * 停用 from 到 to 的调用服务权限
     *
     * @param clazz 服务类别
     * @param from  调用者
     * @param to    被调用者
     */
    public static void disable(Class<?> clazz, int from, int to) {
        String name = clazz.getName();
        if (!net.containsKey(name)) {
            net.put(name, new HashSet<>());
        }
        net.get(name).add(new Pair(from, to));
    }

}
