package com.sakame;

import com.sakame.common.CommandType;
import com.sakame.constant.TimeConstant;
import com.sakame.model.Clerk;
import com.sakame.model.Command;
import com.sakame.model.dto.CommandRequest;
import com.sakame.model.dto.CommandResponse;
import com.sakame.service.KVServerService;
import com.sakame.utils.RpcUtils;

import java.time.LocalDateTime;

/**
 * @author sakame
 * @version 1.0
 */
public class Client {

    private final Clerk clerk = new Clerk();

    Client(KVServerService[] services) {
        clerk.setServices(services);
        clerk.setClientId((int) ((Math.random() * 9 + 1) * 100000));
        clerk.setLeaderId(0);
    }

    /**
     * 发送一条命令
     *
     * @param key   键值
     * @param value 值
     * @param type  命令类型
     * @return 若为 GET 则返回 value
     */
    public String sendCommand(String key, String value, CommandType type) {
        clerk.setSeqId(clerk.getSeqId() + 1);
        Command command = new Command();
        command.setClientId(clerk.getClientId());
        command.setSeqId(clerk.getSeqId());
        command.setKey(key);
        command.setValue(value);
        command.setType(type);
        CommandRequest request = new CommandRequest();
        request.setCommand(command);

        // 10 秒内等待来自 server 的 response
        LocalDateTime time = LocalDateTime.now().plusSeconds(10);
        while (LocalDateTime.now().isBefore(time)) {
            CommandResponse response = RpcUtils.call(clerk.getClientId(), clerk.getLeaderId(), clerk.getServices()[clerk.getLeaderId()], "requestCommand", request);
            if (response != null && response.isSuccess()) {
                return response.getValue();
            }
            // 如果失败则遍历 server 找出 leader
            clerk.setLeaderId((clerk.getLeaderId() + 1) % clerk.getServices().length);
            try {
                Thread.sleep(TimeConstant.RETRY_TIMEOUT);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        return "";
    }

    public String get(String key) {
        return sendCommand(key, "", CommandType.GET);
    }

    public void put(String key, String value) {
        sendCommand(key, value, CommandType.PUT);
    }

    public void append(String key, String value) {
        sendCommand(key, value, CommandType.APPEND);
    }

    public int getId() {
        return clerk.getClientId();
    }

}
