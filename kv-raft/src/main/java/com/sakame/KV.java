package com.sakame;

import com.sakame.constant.Message;
import com.sakame.model.Command;

import java.util.HashMap;
import java.util.Map;

/**
 * @author sakame
 * @version 1.0
 */
public class KV {

    private final Map<String, String> kvMap = new HashMap<>();

    /**
     * 新建或重置键值对
     *
     * @param key
     * @param value
     * @return
     */
    public boolean put(String key, String value) {
        return kvMap.put(key, value) == null;
    }

    /**
     * key 存在则拼接旧值与 value
     *
     * @param key
     * @param value
     * @return
     */
    public boolean append(String key, String value) {
        String pre = kvMap.putIfAbsent(key, value);
        if (pre != null) {
            kvMap.put(key, pre + value);
        }
        return true;
    }

    /**
     * 获取值
     *
     * @param key
     * @return
     */
    public String get(String key) {
        return kvMap.get(key);
    }

    /**
     * 对外操作接口
     *
     * @param command
     * @return
     */
    public String opt(Command command) {
        String key = command.getKey();
        String value = command.getValue();
        switch (command.getType()) {
            case PUT:
                return put(key, value) ? Message.OK : Message.KEY_EXIST;
            case APPEND:
                return append(key, value) ? Message.OK : Message.NO_KEY;
            case GET:
                String ret = get(key);
                return ret == null ? Message.NO_KEY : ret;
            default:
                break;
        }
        return Message.OK;
    }

}