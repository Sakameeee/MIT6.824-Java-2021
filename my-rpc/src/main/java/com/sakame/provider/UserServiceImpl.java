package com.sakame.provider;

import com.sakame.model.User;
import com.sakame.service.UserService;

/**
 * @author sakame
 * @version 1.0
 */
public class UserServiceImpl implements UserService {
    @Override
    public User getUser(User user) {
        System.out.println(user.getName());
        return user;
    }

    @Override
    public int getNumber() {
        return 1;
    }
}
