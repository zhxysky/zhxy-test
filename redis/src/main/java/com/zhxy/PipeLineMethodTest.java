package com.zhxy;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * pipeline与普通方法对比
 * Created by zhxy on 1/6/2017.
 */
public class PipeLineMethodTest {

    public static Jedis getJedis() {
        Jedis jedis = new Jedis("127.0.0.1",6379,40000000);
        jedis.select(4);
        return jedis;
    }

    public static void main(String[] args) throws IOException {
        PipeLineMethodTest test = new PipeLineMethodTest();
        long start;
        long end;
        //List
//        start = System.currentTimeMillis();
//        test.pipeLineListSetTest();
//        test.pipeLineListGetTest();
//        end = System.currentTimeMillis();
//        System.out.println("pipeline list set and get cost : [" + (end - start) + "] ms");

//        start = System.currentTimeMillis();
//        test.listSetTest();
//        test.listGetTest();
//        end = System.currentTimeMillis();
//        System.out.println("jedis set and get cost : [" + (end - start) + "] ms");

        //HashMap
//        start = System.currentTimeMillis();
//        test.pipeLineHmSetTest();
//        test.pipeLineHmGetTest();
//        end = System.currentTimeMillis();
//        System.out.println("pipeline hm set and get cost : [" + (end - start) + "] ms");
//
//
//        start = System.currentTimeMillis();
//        test.hmSetTest();
//        test.hmGetTest();
//        end = System.currentTimeMillis();
//        System.out.println("jedis hm set and get cost : [" + (end - start) + "] ms");


        //SET
//        start = System.currentTimeMillis();
//        test.pipeLineSetSetTest();
//        test.pipeLineSetGetTest();
//        end = System.currentTimeMillis();
//        System.out.println("pipeline set set and get cost : [" + (end - start) + "] ms");
//
//        start = System.currentTimeMillis();
//        test.setSetTest();
//        test.setGetTest();
//        end = System.currentTimeMillis();
//        System.out.println("jedis set set and get cost : [" + (end - start) + "] ms");

//        //ZSET
//        start = System.currentTimeMillis();
//        test.pipeLineZSetSetTest();
//        test.pipeLineZSetGetTest();
//        end = System.currentTimeMillis();
//        System.out.println("pipeline zset set and get cost : [" + (end - start) + "] ms");
//
//        start = System.currentTimeMillis();
//        test.zsetSetTest();
//        test.zsetGetTest();
//        end = System.currentTimeMillis();
//        System.out.println("jedis zset set and get cost : [" + (end - start) + "] ms");


        //String
//        start = System.currentTimeMillis();
//        test.PipeLineStringSetTest();
//        test.PipeLineStringGetTest();
//        end = System.currentTimeMillis();
//        System.out.println("pipeline String set and get cost : [" + (end - start) + "] ms");
//
//        start = System.currentTimeMillis();
//        test.stringSetTest();
//        test.stringGetTest();
//        end = System.currentTimeMillis();
//        System.out.println("jedis String set and get cost : [" + (end - start) + "] ms");

        start = System.currentTimeMillis();
        test.pipeLineHmSetTest();
        test.pipeLineHmGetTest();
        end = System.currentTimeMillis();
        System.out.println("pipeline String set and get cost : [" + (end - start) + "] ms");

        start = System.currentTimeMillis();
        test.pipeLineHSetTest();
        test.pipeLineHGetTest();
        end = System.currentTimeMillis();
        System.out.println("pipeline String set and get cost : [" + (end - start) + "] ms");
    }

    public void listSetTest() {
        Jedis jedis = getJedis();
        jedis.flushDB();
        int count = 0;
        for(int i=0;i<100;i++) {
            for(int j=0;j<10;j++) {
                User user = new User(count, "zhxy" + count , count);
                jedis.rpush("user" + i, String.valueOf(user));
                count++;
            }
        }
        jedis.close();
    }

    public void listGetTest() {
        Jedis jedis = getJedis();
        Set<String> keys = jedis.keys("*");
        List<List<String>> userList = new ArrayList<>();

        for(String key : keys) {
            userList.add(jedis.lrange(key, 0, 10));
        }
        int c = 0;
        for (List<String> list : userList) {
            for(String s: list) {
                System.out.println(c+++"\t"+s);
            }
            System.out.println("----------------------");
        }
    }

    /**
     * pipiLine list set
     * @throws IOException
     */
    public void pipeLineListSetTest() throws IOException {
        Jedis jedis = getJedis();
        jedis.flushDB();
        Pipeline pipeline = jedis.pipelined();
        int count = 0;
        for(int i=0;i<100;i++) {
            for(int j=0;j<10;j++) {
                User user = new User(count, "zhxy" + count , count);
                pipeline.rpush("user" + i, String.valueOf(user));
                count++;
            }
        }
        pipeline.sync();
        jedis.close();
    }

    public void pipeLineListGetTest() {
        Jedis jedis = getJedis();
        Set<String> keys = jedis.keys("*");
        List<Response<List<String>>> resultList = new ArrayList<>();
        List<List<String>> userList = new ArrayList<>();
        Pipeline pipeline = jedis.pipelined();
        for(String key : keys) {
            resultList.add(pipeline.lrange(key,0,10));  //获取某个key指定范围内的元素
        }
        pipeline.sync();

        for(Response response : resultList) {
            userList.add((List<String>) response.get());
        }
        int c = 0;
        for (List<String> list : userList) {
            for(String s: list) {
                System.out.println(c+++"\t"+s);
            }
            System.out.println("----------------------");
        }
        jedis.close();

    }

    public void pipeLineHmSetTest() {
        Jedis jedis = getJedis();
        jedis.flushDB();
        Pipeline pipeline = jedis.pipelined();
        int count = 0;
        for(int i=0;i<100;i++) {
            for(int j=0;j<10;j++) {
                User user = new User(count, "zhxy" + count , count);
                count++;
                pipeline.hmset("user" + count, user.toMap());
            }
        }
        pipeline.sync();
        jedis.close();
    }

    public void pipeLineHmGetTest() {
        Jedis jedis = getJedis();
        Pipeline pipeline = jedis.pipelined();
        Set<String> keys = jedis.keys("*");
        Map<String,Map<String,String>> resultMap = new HashMap<>();
        Map<String, Response<Map<String, String>>> resultResponseMap = new HashMap<>();
        for(String key : keys) {
            resultResponseMap.put(key, pipeline.hgetAll(key));
        }
        pipeline.sync();
        for (Map.Entry<String, Response<Map<String, String>>> entry : resultResponseMap.entrySet()) {
            resultMap.put(entry.getKey(), entry.getValue().get());
        }

        int count = 0;
        for(Map.Entry<String,Map<String,String>> entry : resultMap.entrySet()) {
            System.out.println(count++ + entry.getKey() + "\t" + entry.getValue());
        }

        jedis.close();
    }

    public void hmSetTest() {
        Jedis jedis = getJedis();
        jedis.flushDB();
        int count = 0;
        for(int i=0;i<100;i++) {
            for(int j=0;j<10;j++) {
                User user = new User(count, "zhxy" + count , count);
                count++;
                jedis.hmset("user" + count, user.toMap());
            }
        }
        jedis.close();
    }

    public void hmGetTest() {
        Jedis jedis = getJedis();
        Pipeline pipeline = jedis.pipelined();
        Set<String> keys = jedis.keys("*");
        Map<String,Map<String,String>> resultMap = new HashMap<>();
        for(String key : keys) {
            resultMap.put(key, jedis.hgetAll(key));

        }
        int count = 0;
        for(Map.Entry<String,Map<String,String>> entry : resultMap.entrySet()) {
            System.out.println(count++ +"\t"+ entry.getKey() + "\t" + entry.getValue());
        }

        jedis.close();
    }


    public void pipeLineHSetTest() {
        Jedis jedis = getJedis();
        jedis.flushDB();
        Pipeline pipeline = jedis.pipelined();
        int count = 0;
        for(int i=0;i<100;i++) {
            for(int j=0;j<10;j++) {
                User user = new User(count, "zhxy" + count , count);
                count++;
                for(Map.Entry<String,String> entry : user.toMap().entrySet()) {
                    pipeline.hset("user" + count, entry.getKey(), entry.getValue());
                }

            }
        }
        pipeline.sync();
        jedis.close();
    }

    public void pipeLineHGetTest() {
        Jedis jedis = getJedis();
        Pipeline pipeline = jedis.pipelined();
        Set<String> keys = jedis.keys("*");
        Map<String,Map<String,String>> resultMap = new HashMap<>();
        Map<String, Response<Map<String, String>>> resultResponseMap = new HashMap<>();
        for(String key : keys) {
            resultResponseMap.put(key, pipeline.hgetAll(key));
        }
        pipeline.sync();
        for (Map.Entry<String, Response<Map<String, String>>> entry : resultResponseMap.entrySet()) {
            resultMap.put(entry.getKey(), entry.getValue().get());
        }

        int count = 0;
        for(Map.Entry<String,Map<String,String>> entry : resultMap.entrySet()) {
            System.out.println(count++ +"\t"+ entry.getKey() + "\t" + entry.getValue());
        }

        jedis.close();
    }

    public void pipeLineSetSetTest() {
        Jedis jedis = getJedis();
        jedis.flushDB();
        Pipeline pipeline = jedis.pipelined();
        int count = 0;
        for(int i=0;i<100;i++) {
            for(int j=0;j<10;j++) {
                User user = new User(count, "zhxy" + count , count);
                count++;
                pipeline.sadd("user" + i, String.valueOf(user)); //一个key对应多个user
            }
        }
        pipeline.sync();
        jedis.close();

    }
    public void pipeLineSetGetTest() {
        Jedis jedis = getJedis();
        Set<String> keys = jedis.keys("*");
        List<Response<Set<String>>> resultList = new ArrayList<>();
        List<Set<String>> userList = new ArrayList<>();
        Pipeline pipeline = jedis.pipelined();
        for(String key : keys) {
            resultList.add(pipeline.smembers(key));  //获取某个key指定范围内的元素
        }
        pipeline.sync();

        for(Response response : resultList) {
            userList.add((Set<String>) response.get());
        }
        int c = 0;
        for (Set<String> list : userList) {
            for(String s: list) {
                System.out.println(c+++"\t"+s);
            }
            System.out.println("----------------------");
        }
        jedis.close();
    }

    public void setSetTest() {
        Jedis jedis = getJedis();
        jedis.flushDB();
        int count = 0;
        for(int i=0;i<100;i++) {
            for(int j=0;j<10;j++) {
                User user = new User(count, "zhxy" + count , count);
                count++;
                jedis.sadd("user" + i, String.valueOf(user));
            }
        }
        jedis.close();
    }

    public void setGetTest() {
        Jedis jedis = getJedis();
        Set<String> keys = jedis.keys("*");
        List<Set<String>> userList = new ArrayList<>();
        Pipeline pipeline = jedis.pipelined();
        for(String key : keys) {
            userList.add(jedis.smembers(key));
        }
        pipeline.sync();


        int c = 0;
        for (Set<String> list : userList) {
            for(String s: list) {
                System.out.println(c+++"\t"+s);
            }
            System.out.println("----------------------");
        }
        jedis.close();

    }

    public void pipeLineZSetSetTest() {
        Jedis jedis = getJedis();
        jedis.flushDB();
        Pipeline pipeline = jedis.pipelined();
        int count = 0;
        for(int i=0;i<100;i++) {
            for(int j=0;j<10;j++) {
                User user = new User(count, "zhxy" + count , count);
                count++;
                pipeline.zadd("user" + i, count, String.valueOf(user));
            }
        }
        pipeline.sync();
        jedis.close();

    }

    public void pipeLineZSetGetTest() {
        Jedis jedis = getJedis();
        Set<String> keys = jedis.keys("*");
        List<Response<Set<String>>> resultList = new ArrayList<>();
        List<Set<String>> userList = new ArrayList<>();
        Pipeline pipeline = jedis.pipelined();
        for(String key : keys) {
            resultList.add(pipeline.zrange(key,0,10));  //获取某个key指定范围内的元素
        }
        pipeline.sync();

        for(Response response : resultList) {
            userList.add((Set<String>) response.get());
        }
        int c = 0;
        for (Set<String> list : userList) {
            for(String s: list) {
                System.out.println(c+++"\t"+s);
            }
            System.out.println("----------------------");
        }
        jedis.close();
    }

    public void zsetSetTest() {
        Jedis jedis = getJedis();
        jedis.flushDB();
        int count = 0;
        for(int i=0;i<100;i++) {
            for(int j=0;j<10;j++) {
                User user = new User(count, "zhxy" + count , count);
                count++;
                jedis.zadd("user" + i,count, String.valueOf(user));
            }
        }
        jedis.close();
    }

    public void zsetGetTest() {
        Jedis jedis = getJedis();
        Set<String> keys = jedis.keys("*");
        List<Set<String>> userList = new ArrayList<>();
        Pipeline pipeline = jedis.pipelined();
        for(String key : keys) {
            userList.add(jedis.zrange(key,0,10));
        }
        pipeline.sync();


        int c = 0;
        for (Set<String> list : userList) {
            for(String s: list) {
                System.out.println(c+++"\t"+s);
            }
            System.out.println("----------------------");
        }
        jedis.close();
    }


    public void PipeLineStringSetTest() {
        Jedis jedis = getJedis();
        jedis.flushDB();
        Pipeline pipeline = jedis.pipelined();
        int count = 0;
        for(int i=0;i<100;i++) {
            for(int j=0;j<10;j++) {
                User user = new User(count, "zhxy" + count , count);
                count++;
                pipeline.set("user" + count, String.valueOf(user));
            }
        }
        pipeline.sync();
        jedis.close();
    }

    public void PipeLineStringGetTest() {
        Jedis jedis = getJedis();
        Set<String> keys = jedis.keys("*");
        List<Response<String>> responsesList = new ArrayList<>();
        List<String> userList = new ArrayList<>();
        Pipeline pipeline = jedis.pipelined();
        for(String key : keys) {
            responsesList.add(pipeline.get(key));
        }
        pipeline.sync();

        for(Response<String> response : responsesList) {
            userList.add(response.get());
        }

        int c = 0;
        for (String user : userList) {
            System.out.println(c+++"\t"+user);
        }
        jedis.close();
    }


    public void stringSetTest() {
        Jedis jedis = getJedis();
        jedis.flushDB();
        int count = 0;
        for(int i=0;i<100;i++) {
            for(int j=0;j<10;j++) {
                User user = new User(count, "zhxy" + count , count);
                count++;
                jedis.set("user" + count, String.valueOf(user));
            }
        }
        jedis.close();
    }

    public void stringGetTest() {
        Jedis jedis = getJedis();
        Set<String> keys = jedis.keys("*");
        List<String> userList = new ArrayList<>();
        for(String key : keys) {
            userList.add(jedis.get(key));
        }

        int c = 0;
        for (String user : userList) {
            System.out.println(c+++"\t"+user);
        }
        jedis.close();
    }
}
