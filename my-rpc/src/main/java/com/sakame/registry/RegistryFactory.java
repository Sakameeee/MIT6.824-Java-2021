package com.sakame.registry;

import java.util.HashMap;
import java.util.Map;

/**
 * 注册中心工厂
 *
 * @author sakame
 * @version 1.0
 */
public class RegistryFactory {

    private static final Map<String, Registry> KEY_REGISTRY_MAP = new HashMap<>() {{
        put("etcd", new EtcdRegistry());
    }};

    public static Registry getInstance(String key) {
        return KEY_REGISTRY_MAP.get(key);
    }

}
