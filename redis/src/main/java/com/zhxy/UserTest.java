package com.zhxy;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhxy on 1/3/2017.
 */
public class UserTest {
    public static void main(String[] args) {

        System.out.println("test start....");

        Jedis jedis = new Jedis("127.0.0.1",6379,40000000);

        Map<String, String> data = new HashMap<String, String>();
        jedis.select(8);
        jedis.flushDB();
        long start = System.currentTimeMillis();
        User user ;
        //set data
        for(int i=0;i<10000;i++) {
            user = new User(i,"zhxy_"+i,i);
            jedis.hset("user_" + i, "id", user.getId()+"");
            jedis.hset("user_" + i, "name", user.getName());
            jedis.hset("user_" + i, "age", user.getAge()+"");
        }
        long end = System.currentTimeMillis();

        System.out.println("dbsize:["+jedis.dbSize()+"]");
        System.out.println("hmset without pipeline used [" + (end - start)  + "] ms");

        jedis.select(8);
        jedis.flushDB();

        //set data with pipeLine
        Pipeline pipeline = jedis.pipelined();
        start = System.currentTimeMillis();
        for(int i=0;i<10000;i++) {
            user = new User(i,"zhxy_"+i,i);
            pipeline.hset("user_" + i, "id", user.getId()+"");
            pipeline.hset("user_" + i, "name", user.getName());
            pipeline.hset("user_" + i, "age", user.getAge()+"");
        }

        pipeline.sync();
        end = System.currentTimeMillis();
        System.out.println("dbsize:["+jedis.dbSize()+"]");
        System.out.println("hmset with pipeline used [" + (end - start) + "] ms");
        System.out.println("test end....");


        //get data
        Set keys = jedis.keys("*");
        start = System.currentTimeMillis();
        Map<String, String> result = new HashMap<String, String>();
        for (Object key : keys) {
            result.put((String) key, jedis.hget((String) key,"id"));
        }
        end = System.currentTimeMillis();
        System.out.println("result size:[" + result.size() + "]");
        System.out.println("hgetall without pipeline used [" + (end - start) + "] ms");

        //get data with pipeline
        Map<String, Response<String>> responses = new HashMap<String, Response<String>>(keys.size());
        result.clear();
        start = System.currentTimeMillis();
        for (Object key : keys) {
            responses.put((String) key, pipeline.hget((String) key, "id"));
        }
        pipeline.sync();
        for (String k : responses.keySet()) {
            result.put(k, responses.get(k).get());
        }
        end = System.currentTimeMillis();
        System.out.println("result size:[" + result.size() + "]");
        System.out.println("hgetall with pipeline used [" + (end - start) + "] ms");

        int count = 0 ;
        for( String k : result.keySet()) {
            System.out.println(k +"\t"+result.get(k));
            count ++;
        }
        System.out.println("count:"+count);
        jedis.disconnect();

    }
}
