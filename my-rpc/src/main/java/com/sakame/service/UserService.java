package com.sakame.service;

import com.sakame.model.User;

/**
 * @author sakame
 * @version 1.0
 */
public interface UserService {
    /**
     * 获取用户
     *
     * @param user
     * @return
     */
    User getUser(User user);

    int getNumber();
}
