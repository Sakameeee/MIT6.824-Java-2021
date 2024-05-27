package com.sakame.registry;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.collection.ConcurrentHashSet;
import cn.hutool.cron.CronUtil;
import cn.hutool.cron.task.Task;
import cn.hutool.json.JSONUtil;
import com.sakame.config.RegistryConfig;
import com.sakame.model.ServiceMetaInfo;
import io.etcd.jetcd.*;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.watch.WatchEvent;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Etcd 实现服务注册中心
 *
 * @author sakame
 * @version 1.0
 */
public class EtcdRegistry implements Registry {

    /**
     * 服务端已注册服务的 key (服务端维护)
     */
    private static final Set<String> localRegisterNodeKeySet = new HashSet<>();
    /**
     * 存储的根目录
     */
    private static final String ETCD_ROOT_PATH = "/rpc/";
    /**
     * 要监听的服务键
     */
    private final Set<String> watchingKeySet = new ConcurrentHashSet<>();
    /**
     * 已注册服务缓存，(消费端维护)，服务名:版本号 => 服务节点列表
     */
    private final Map<String, List<ServiceMetaInfo>> registryServiceCache = new HashMap<>();
    private Client client;
    private KV kvClient;

    @Override
    public void init(RegistryConfig registryConfig) {
        client = Client.builder()
                .endpoints(registryConfig.getAddress())
                .connectTimeout(Duration.ofMillis(registryConfig.getTimeout()))
                .build();
        kvClient = client.getKVClient();
        heartbeat();
    }

    @Override
    public void register(ServiceMetaInfo serviceMetaInfo) throws Exception {
        // 创建 lease 客户端
        Lease leaseClient = client.getLeaseClient();

        // 创建一个 30 秒的租约
        long leaseId = leaseClient.grant(30).get().getID();

        // 设置要存储的键值对
        String regitryKey = ETCD_ROOT_PATH + serviceMetaInfo.getServiceNodeKey();
        ByteSequence key = ByteSequence.from(regitryKey, StandardCharsets.UTF_8);
        ByteSequence value = ByteSequence.from(JSONUtil.toJsonStr(serviceMetaInfo), StandardCharsets.UTF_8);

        // 将键值对和租约关联起来并设置过期时间
        PutOption putOption = PutOption.builder()
                .withLeaseId(leaseId)
                .build();
        kvClient.put(key, value, putOption).get();
        localRegisterNodeKeySet.add(regitryKey);
    }

    @Override
    public void unRegister(ServiceMetaInfo serviceMetaInfo) {
        String registryKey = ETCD_ROOT_PATH + serviceMetaInfo.getServiceNodeKey();
        localRegisterNodeKeySet.remove(registryKey);
        kvClient.delete(ByteSequence.from(registryKey, StandardCharsets.UTF_8));
    }

    @Override
    public List<ServiceMetaInfo> serviceDiscovery(String serviceKey) {
        List<ServiceMetaInfo> list = registryServiceCache.get(serviceKey);
        if (!CollUtil.isEmpty(list)) {
            return list;
        }

        String searchPrefix = ETCD_ROOT_PATH + serviceKey + "/";

        try {
            GetOption getOption = GetOption.builder()
                    .isPrefix(true)
                    .build();
            List<KeyValue> keyValues = kvClient.get(
                            ByteSequence.from(searchPrefix, StandardCharsets.UTF_8),
                            getOption)
                    .get()
                    .getKvs();

            List<ServiceMetaInfo> serviceMetaInfoList = keyValues.stream()
                    .map(keyValue -> {
                        String value = keyValue.getValue().toString(StandardCharsets.UTF_8);
                        String registryKey = keyValue.getKey().toString(StandardCharsets.UTF_8);
                        watch(registryKey);
                        return JSONUtil.toBean(value, ServiceMetaInfo.class);
                    })
                    .collect(Collectors.toList());
            // todo:缓存更新策略
            registryServiceCache.put(serviceKey, serviceMetaInfoList);
            return serviceMetaInfoList;
        } catch (Exception e) {
            throw new RuntimeException("获取服务列表失败", e);
        }
    }

    @Override
    public void destroy() {
        for (String key : localRegisterNodeKeySet) {
            try {
                kvClient.delete(ByteSequence.from(key, StandardCharsets.UTF_8));
            } catch (Exception e) {
                throw new RuntimeException("fail to shutdown node: " + key);
            }
        }

        if (kvClient != null) {
            kvClient.close();
        }
        if (client != null) {
            client.close();
        }
    }

    /**
     * 针对单个服务节点（服务提供者）
     */
    @Override
    public void heartbeat() {
        CronUtil.schedule("*/10 * * * * *", (Task) () -> {
            for (String key : localRegisterNodeKeySet) {
                try {
                    List<KeyValue> kvs = kvClient.get(ByteSequence.from(key, StandardCharsets.UTF_8))
                            .get()
                            .getKvs();
                    if (CollUtil.isEmpty(kvs)) {
                        continue;
                    }
                    KeyValue keyValue = kvs.get(0);
                    String value = keyValue.getValue().toString();
                    ServiceMetaInfo serviceMetaInfo = JSONUtil.toBean(value, ServiceMetaInfo.class);
                    register(serviceMetaInfo);
                } catch (Exception e) {
                    throw new RuntimeException(key + "fail to expire key", e);
                }
            }
        });

        CronUtil.setMatchSecond(true);
        CronUtil.start();
    }

    /**
     * 监听单个服务节点而非整个服务列表（消费者）
     *
     * @param registryKey
     */
    @Override
    public void watch(String registryKey) {
        Watch watchClient = client.getWatchClient();
        boolean add = watchingKeySet.add(registryKey);
        if (add) {
            watchClient.watch(ByteSequence.from(registryKey, StandardCharsets.UTF_8), watchResponse -> {
                for (WatchEvent event : watchResponse.getEvents()) {
                    String[] strings = registryKey.split("/");
                    String serviceKey = strings[2];
                    String serviceNodeKey = serviceKey + "/" + strings[3];
                    List<ServiceMetaInfo> list = registryServiceCache.get(serviceKey);
                    if (event.getEventType() == WatchEvent.EventType.DELETE) {
                        for (ServiceMetaInfo serviceMetaInfo : list) {
                            if (serviceMetaInfo.getServiceNodeKey().equals(serviceNodeKey)) {
                                list.remove(serviceMetaInfo);
                                break;
                            }
                        }
                    }
                }
            });
        }
    }
}
