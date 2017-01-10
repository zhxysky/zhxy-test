package com.zhxy;

import com.alibaba.fastjson.JSON;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhxy on 1/9/2017.
 */
public class PipeLineUtil<T> {

    public static Jedis getJedis() {
        Jedis jedis = new Jedis("127.0.0.1",6379,40000000);
        jedis.select(4);
        return jedis;
    }

    /**
     * 根据key列表获取key对应的所有属性
     * @param keys key列表
     * @return
     */
    public static Map<String,Map<String,String>> hGetWithPipeLine(List<String> keys) {
        Map<String, Map<String, String>> resultMap = new HashMap<>();
        Map<String, Response<Map<String, String>>> responseMap = new HashMap<>();
        Jedis jedis = null;
        boolean broken = false;

        try {
            jedis = getJedis();

            Pipeline pipeline = jedis.pipelined();
            for(String key : keys) {
                responseMap.put(key, pipeline.hgetAll(key));
            }
            pipeline.sync();
            for(Map.Entry<String, Response<Map<String, String>>> entry : responseMap.entrySet()) {
                resultMap.put(entry.getKey(), entry.getValue().get());
            }
            return resultMap;
        } catch (JedisConnectionException e) {
            broken = true;
            throw e;
        } finally {
            jedis.close();
        }

    }

    /**
     * 根据key和field获取对应属性值
     * @param keyFieldMap 要获取结果的key和field列表的map map<Key,List<Field>>
     * @return key和对应结果的map
     */
    public static Map<String, Map<String, String>> hGetWithPipeLine(Map<String, List<String>> keyFieldMap) {

        // Map<Key,Map<Field,Value>
        Map<String, Map<String,String>> result = new HashMap<>();
        // Map<key,Map<Field,Response<Value>>
        Map<String, Map<String,Response<String>>> responseMap = new HashMap<>();

        Jedis jedis = null;
        boolean broken = false;
        try {
            jedis = getJedis();
            Pipeline pipeline = jedis.pipelined();
            String key;
            for (Map.Entry<String,List<String>> entry : keyFieldMap.entrySet()) {
                for(String field : entry.getValue()) {
                    key = entry.getKey();
                    if(responseMap.containsKey(key) ) {
                        responseMap.get(entry.getKey()).put(field, pipeline.hget(key, field));
                    }else {
                        Map<String, Response<String>> fieldValueMap = new HashMap<>();
                        fieldValueMap.put(field, pipeline.hget(key, field));
                        responseMap.put(key, fieldValueMap);
                    }
                }
            }
            pipeline.sync();
            for(Map.Entry<String, Map<String, Response<String>>> entry : responseMap.entrySet()) {
                key = entry.getKey();
                for(Map.Entry<String,Response<String>> e : entry.getValue().entrySet()) {
                    if(result.containsKey(key)) {
                        result.get(key).put(e.getKey(), e.getValue().get());
                    }else {
                        Map<String, String> fieldValueMap = new HashMap<>();
                        fieldValueMap.put(e.getKey(), e.getValue().get());
                        result.put(key, fieldValueMap);
                    }

                }
            }

            return result;
        } catch (JedisConnectionException e) {
            broken = true;
            throw e;
        } finally {
            jedis.close();
        }

    }


    /**
     * 使用PipeLine批量设置内容到redis
     * @param map  要设置的参数内容，格式为Map<Key,Map<FieldName,FieldValue>>
     */
    public static void hmSetWithPipeLine(Map<String,Map<String,String>> map) {
        Jedis jedis = null;
        boolean broken = false;
        try {
            jedis = getJedis();
            jedis.flushDB();
            Pipeline pipeline = jedis.pipelined();
            for (Map.Entry<String,Map<String,String>> entry : map.entrySet()) {
                pipeline.hmset(entry.getKey(), entry.getValue());
            }
            pipeline.sync();
        } catch (JedisConnectionException e) {
            broken = true;
            throw e;
        } finally {
            jedis.close();
        }
    }

    /**
     * 批量设置key-value
     * @param map key和value的map map<Key,Value>
     */
    public void setWithPipeLine(Map<String,Object> map) {
        Jedis jedis = null;
        boolean broken = false;
        try {
            jedis = getJedis();
            jedis.flushDB();
            Pipeline pipeline = jedis.pipelined();
            for (Map.Entry<String,Object> entry : map.entrySet()) {
                pipeline.set(entry.getKey(), JSON.toJSONString(entry.getValue()));
            }
            pipeline.sync();
        } catch (JedisConnectionException e) {
            broken = true;
            throw e;
        } finally {
            jedis.close();
        }
    }


    /**
     * 批量获取key对应的值
     * @param keys key列表
     */
    public Map<String, Object > getWithPipeLine(List<String> keys, Class<T> itemType) {
        Map<String, Object> resultMap = new HashMap<>();
        Map<String, Response<String>> responseMap = new HashMap<>();
        Jedis jedis = null;
        boolean broken = false;

        try {
            jedis = getJedis();
            Pipeline pipeline = jedis.pipelined();
            for(String key : keys) {
                responseMap.put(key, pipeline.get(key));
            }
            pipeline.sync();
            for(Map.Entry<String, Response<String>> entry : responseMap.entrySet()) {
                resultMap.put(entry.getKey(), JSON.parseObject(entry.getValue().get(),itemType));
            }
            return resultMap;
        } catch (JedisConnectionException e) {
            broken = true;
            throw e;
        } finally {
            jedis.close();
        }
    }


    /**
     * 批量设置key-value
     * @param map key和value的map map<Key,Value>
     */
    public void sAddWithPipeLine(Map<String,Set<Object>> map) {
        Jedis jedis = null;
        boolean broken = false;
        try {
            jedis = getJedis();
            jedis.flushDB();
            Pipeline pipeline = jedis.pipelined();
            for (Map.Entry<String,Set<Object>> entry : map.entrySet()) {
                for(Object obj : entry.getValue()){
                    pipeline.sadd(entry.getKey(), JSON.toJSONString(obj));
                }
            }
            pipeline.sync();
        } catch (JedisConnectionException e) {
            broken = true;
            throw e;
        } finally {
            jedis.close();
        }
    }

    public Map<String,Set<Object>>  sMembersWithPipeLine(List<String> keys,Class itemType) {
        Jedis jedis = null;
        boolean broken = false;
        try {
            jedis = getJedis();
            Pipeline pipeline = jedis.pipelined();
            Map<String, Response<Set<String>>> responseMap = new HashMap<>();
            Map<String, Set<Object>> resultMap = new HashMap<>();
            for(String key : keys) {
                responseMap.put(key, pipeline.smembers(key));
            }
            pipeline.sync();

            for (Map.Entry<String,Response<Set<String>>> entry : responseMap.entrySet()) {
                Set<Object> set = new HashSet<>();
                for(String json : entry.getValue().get()) {
                    set.add(JSON.parseObject(json , itemType));
                }
                resultMap.put(entry.getKey(), set);
            }
            return resultMap;
        } catch (JedisConnectionException e) {
            broken = true;
            throw e;
        } finally {
            jedis.close();
        }
    }

    /**
     * SortedSet 格式
     * 批量设置key-value
     * @param map key和value以及score的map map<Key,Map<Value,Score>>
     */
    public void zAddWithPipeLine(Map<String,Map<Object,Double>> map) {
        Jedis jedis = null;
        boolean broken = false;
        try {
            jedis = getJedis();
            jedis.flushDB();
            Pipeline pipeline = jedis.pipelined();
            for (Map.Entry<String,Map<Object,Double>> entry : map.entrySet()) {
                for(Map.Entry<Object,Double> e : entry.getValue().entrySet()) {
                    pipeline.zadd(entry.getKey(), e.getValue(), JSON.toJSONString(e.getKey()));
                }
            }
            pipeline.sync();
        } catch (JedisConnectionException e) {
            broken = true;
            throw e;
        } finally {
            jedis.close();
        }
    }

    /**
     *
     * @param keys
     * @param itemType
     * @param start 要获取的开始位置
     * @param end   要获取的结束位置
     * @param reverseOrder 是否逆序 1为逆序 0为正序
     * @return
     */
    public Map<String,List<Object>>  zrangeWithPipeLine(List<String> keys,Class itemType,int start,int end ,int reverseOrder) {
        Jedis jedis = null;
        boolean broken = false;
        try {
            jedis = getJedis();
            Pipeline pipeline = jedis.pipelined();
            Map<String, Response<Set<String>>> responseMap = new HashMap<>();
            Map<String, List<Object>> resultMap = new HashMap<>();
            for(String key : keys) {
                if(reverseOrder == 1) {
                    responseMap.put(key,pipeline.zrevrange(key, start, end));
                }else {
                    responseMap.put(key,pipeline.zrange(key, start, end));
                }
            }
            pipeline.sync();

            for (Map.Entry<String,Response<Set<String>>> entry : responseMap.entrySet()) {
                List<Object> list = new ArrayList<>();
                for(String json : entry.getValue().get()) {
                    list.add(JSON.parseObject(json , itemType));
                }
                resultMap.put(entry.getKey(), list);
            }
            return resultMap;
        } catch (JedisConnectionException e) {
            broken = true;
            throw e;
        } finally {
            jedis.close();
        }
    }

    /**
     * List 格式
     * 批量设置key-value
     * @param map key和value列表的map map<Key,List<Value>>
     * @param  type  添加类型： type=0 表示 rpush , type=1 表示 lpush
     */
    public void listPushWithPipeLine(Map<String,List<Object>> map, int type) {
        Jedis jedis = null;
        boolean broken = false;
        try {
            jedis = getJedis();
            jedis.flushDB();
            Pipeline pipeline = jedis.pipelined();
            for (Map.Entry<String,List<Object>> entry : map.entrySet()) {
                for(Object value : entry.getValue()) {
                    if(type == 0) {
                        pipeline.rpush(entry.getKey(), JSON.toJSONString(value));
                    }else{
                        pipeline.lpush(entry.getKey(), JSON.toJSONString(value));
                    }
                }
            }
            pipeline.sync();
        } catch (JedisConnectionException e) {
            broken = true;
            throw e;
        } finally {
            jedis.close();
        }
    }

    /**
     *
     * @param keys
     * @param itemType 要获取的对象的类型
     * @param start 要获取的开始位置
     * @param end   要获取的结束位置
     * @return
     */
    public Map<String,List<Object>>  listRangeWithPipeLine(List<String> keys,Class itemType,int start,int end ) {
        Jedis jedis = null;
        boolean broken = false;
        try {
            jedis = getJedis();
            Pipeline pipeline = jedis.pipelined();
            Map<String, Response<List<String>>> responseMap = new HashMap<>();
            Map<String, List<Object>> resultMap = new HashMap<>();
            for(String key : keys) {
                responseMap.put(key,pipeline.lrange(key, start, end));
            }
            pipeline.sync();

            for (Map.Entry<String,Response<List<String>>> entry : responseMap.entrySet()) {
                List<Object> list = new ArrayList<>();
                for(String json : entry.getValue().get()) {
                    list.add(JSON.parseObject(json , itemType));
                }
                resultMap.put(entry.getKey(), list);
            }
            return resultMap;
        } catch (JedisConnectionException e) {
            broken = true;
            throw e;
        } finally {
            jedis.close();
        }
    }

    public static void main(String[] args) {
        Map<String, Map<String, String>> userMap = new HashMap<>();
        Map<String, List<String>> keyFieldMap = new HashMap<>();
        Map<String, Object> userObjMap = new HashMap<>();
        Map<String, Set<Object>> userSetMap = new HashMap<>();
        Map<String, List<Object>> userListMap = new HashMap<>();
        Map<String, Map<Object,Double>> userScoreSetMap = new HashMap<>();
        List<String> keys = new ArrayList<>();
        String key;
        for(int i=0;i<100;i++) {
            key = "user_"+i;
            User user = new User(i,"zhxy_"+i,i);
            userObjMap.put(key, user);
            userMap.put(key, user.toMap());
            List<String> fieldList = new ArrayList<>();
            Field[] fields = user.getClass().getDeclaredFields();
            for(Field field : fields) {
                fieldList.add(field.getName());
            }
            keyFieldMap.put(key, fieldList);
            keys.add(key);
            Set<Object> userSet = new HashSet<>();
            List<Object> userList = new ArrayList<>();
            Map<Object, Double> scoreMap = new HashMap<>();
            for(int j=0;j<10;j++) {
                User u = new User(j, "zhxy_" + j, j);
                userSet.add(u);
                userList.add(u);
                scoreMap.put(u, (double) j);

            }
            userSetMap.put(key, userSet);
            userListMap.put(key, userList);
            userScoreSetMap.put(key, scoreMap);
        }

//        hmSetWithPipeLine(userMap);

        int count = 0;
//        Map<String, Map<String, String>> result = hGetWithPipeLine(keyFieldMap);
//        Map<String, Map<String, String>> result = hGetWithPipeLine(keys);
//        for(Map.Entry<String,Map<String,String>> entry : result.entrySet()) {
//            StringBuilder stringBuilder = new StringBuilder();
//            stringBuilder.append(count++ + "\t");
//            stringBuilder.append(entry.getKey() + "\t");
//            for(Map.Entry<String,String> e: entry.getValue().entrySet()) {
//                stringBuilder.append(e.getKey() + "=" + e.getValue() + "\t");
//            }
//            System.out.println(stringBuilder.toString());
//        }

        PipeLineUtil util = new PipeLineUtil();
//        util.setWithPipeLine(userObjMap);
//        Map<String, User> map = util.getWithPipeLine(keys, User.class);
//        for(Map.Entry<String,User> entry : map.entrySet()) {
//            StringBuilder stringBuilder = new StringBuilder();
//            stringBuilder.append(count++ + "\t");
//            stringBuilder.append(entry.getKey() + "\t" + entry.getValue());
//
//            System.out.println(stringBuilder.toString());
//        }


//        util.sAddWithPipeLine(userSetMap);
//
//        Map<String,Set<Object>> resultMap = util.sMembersWithPipeLine(keys, User.class);
//        for(Map.Entry<String,Set<Object>> entry : resultMap.entrySet()) {
//            int i = 0;
//            for(Object obj : entry.getValue()) {
//                System.out.println(entry.getKey()+"\t"+ i++ +JSON.toJSONString(obj));
//            }
//            System.out.println();
//            System.out.println();
//        }


//        util.zAddWithPipeLine(userScoreSetMap);

//        Map<String,List<Object>> resultMap = util.zrangeWithPipeLine(keys, User.class , 0, 6 , 1);
//        for(Map.Entry<String,List<Object>> entry : resultMap.entrySet()) {
//            int i = 0;
//            for(Object obj : entry.getValue()) {
//                System.out.println(entry.getKey()+"\t"+ i++ +JSON.toJSONString(obj));
//            }
//            System.out.println();
//            System.out.println();
//        }


        util.listPushWithPipeLine(userListMap , 1);

        Map<String,List<Object>> resultMap = util.listRangeWithPipeLine(keys, User.class , 0, 6);
        for(Map.Entry<String,List<Object>> entry : resultMap.entrySet()) {
            int i = 0;
            for(Object obj : entry.getValue()) {
                System.out.println(entry.getKey()+"\t"+ i++ +JSON.toJSONString(obj));
            }
            System.out.println();
            System.out.println();
        }

    }
}
