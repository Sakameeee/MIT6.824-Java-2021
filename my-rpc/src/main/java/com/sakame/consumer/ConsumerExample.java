package com.sakame.consumer;

import com.sakame.model.User;
import com.sakame.proxy.ServiceProxyFactory;
import com.sakame.service.UserService;

/**
 * @author sakame
 * @version 1.0
 */
public class ConsumerExample {
    public static void main(String[] args) {
        UserService userService = ServiceProxyFactory.getProxy(UserService.class);
        User user = new User();
        user.setName("sakame");
        User newUser = userService.getUser(user);
        if (newUser != null) {
            System.out.println(newUser.getName());
        } else {
            System.out.println("error");
        }
    }
}
