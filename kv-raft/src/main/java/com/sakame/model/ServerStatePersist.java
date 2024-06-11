package com.sakame.model;

import com.sakame.KV;
import com.sakame.common.CommandContext;
import lombok.Data;

import java.util.Map;

/**
 * @author sakame
 * @version 1.0
 */
@Data
public class ServerStatePersist {

    private KV kvMap;

    private Map<Integer, CommandContext> lastCmdContext;

}
