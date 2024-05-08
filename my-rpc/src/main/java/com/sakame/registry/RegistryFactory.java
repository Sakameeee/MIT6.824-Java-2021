package com.sakame.registry;

/**
 * 注册中心工厂
 * @author sakame
 * @version 1.0
 */
public class RegistryFactory {

    public static Registry getInstance(String key) {
        switch (key) {
            case "etcd":
                return new EtcdRegistry();
            default:
                break;
        }
        return new EtcdRegistry();
    }

}
