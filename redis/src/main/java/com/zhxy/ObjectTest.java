package com.zhxy;

import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import redis.clients.jedis.Jedis;

/**
 * Created by zhxy on 1/4/2017.
 *
 * 测试redis存储对象
 */
public class ObjectTest {

    public static void main(String[] args) {
        Jedis jedis = new Jedis("127.0.0.1",6379,40000000);

        jedis.select(7);
        User user = new User(2, "zhxy2", 30);
        JdkSerializationRedisSerializer serializer = new JdkSerializationRedisSerializer();

        jedis.set("user2".getBytes(), serializer.serialize(user));
        User u2 = (User) serializer.deserialize(jedis.get("user2".getBytes()));
        System.out.println("u2:"+u2);

        User u = (User) serializer.deserialize(jedis.get("user1".getBytes()));
        System.out.println("u:"+u);

        jedis.disconnect();
    }
}
