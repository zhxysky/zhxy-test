package com.zhxy;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Created by zhxy on 1/3/2017.
 */
public class PipeLineTest {
    public static void main(String[] args) {
        System.out.println("test start....");

        Jedis jedis = new Jedis("127.0.0.1",6379,40000000);

        Map<String, String> data = new HashMap<String, String>();
        jedis.select(8);
        jedis.flushDB();
        long start = System.currentTimeMillis();
        //set data
        for(int i=0;i<10000;i++) {
            data.clear();
            data.put("k_" + i, "v_" + i);
            jedis.hmset("key_" + i, data);
        }
        long end = System.currentTimeMillis();

        System.out.println("dbsize:["+jedis.dbSize()+"]");
        System.out.println("hmset without pipeline used [" + (end - start)  + "] ms");

        jedis.select(8);
        jedis.flushDB();

        Random random = new Random();

        //set data with pipeLine
        Pipeline pipeline = jedis.pipelined();
        start = System.currentTimeMillis();
        for(int i=0;i<10000;i++) {
            data.clear();
            data.put("k_" + i, "v_" + i);
            pipeline.hmset("key_" + i, data);
        }

        pipeline.sync();
        end = System.currentTimeMillis();
        System.out.println("dbsize:["+jedis.dbSize()+"]");
        System.out.println("hmset with pipeline used [" + (end - start) + "] ms");
        System.out.println("test end....");


        //get data
        Set keys = jedis.keys("*");
        start = System.currentTimeMillis();
        Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();
        for (Object key : keys) {
            result.put((String) key, jedis.hgetAll((String) key));
        }
        end = System.currentTimeMillis();
        System.out.println("result size:[" + result.size() + "]");
        System.out.println("hgetall without pipeline used [" + (end - start) + "] ms");

        //get data with pipeline
        Map<String, Response<Map<String, String>>> responses = new HashMap<String, Response<Map<String, String>>>(keys.size());
        result.clear();
        start = System.currentTimeMillis();
        for (Object key : keys) {
            responses.put((String) key, pipeline.hgetAll((String) key));
        }
        pipeline.sync();
        for (String k : responses.keySet()) {
            result.put(k, responses.get(k).get());
        }
        end = System.currentTimeMillis();
        System.out.println("result size:[" + result.size() + "]");
        System.out.println("hgetall with pipeline used [" + (end - start) + "] ms");

        jedis.disconnect();

    }
}
