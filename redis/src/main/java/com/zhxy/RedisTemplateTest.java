package com.zhxy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;

import javax.annotation.Resource;
import java.net.URL;

/**
 * Created by zhxy on 1/5/2017.
 */
public class RedisTemplateTest {

    @Autowired
    private RedisTemplate<String,String> redisTemplate;

    @Resource(name = "redisTemplate")
    private ListOperations<String , String> listOperations;
    private ListOperations<String , Object> listOperations2;

    public void addLink(String userId,URL url) {
        listOperations.leftPush(userId, url.toExternalForm());
        listOperations2.leftPush("aaa", new User(1,"abc",12));
    }



}
