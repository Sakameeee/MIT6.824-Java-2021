package com.sakame.common;

/**
 * 命令类型
 *
 * @author sakame
 * @version 1.0
 */
public enum CommandType {

    GET(0, "get"),
    PUT(1, "put"),
    APPEND(2, "append");

    private final int code;

    private final String description;

    CommandType(int code, String description) {
        this.code = code;
        this.description = description;
    }

    public int getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

}
