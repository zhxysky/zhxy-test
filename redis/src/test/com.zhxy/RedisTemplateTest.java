import com.alibaba.fastjson.JSON;
import com.zhxy.User;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhxy on 1/5/2017.
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:applicationContext.xml"})
public class RedisTemplateTest {

    @Autowired
    private RedisTemplate<Object,Object> redisTemplate;

    @Test
    public void testOpsForValue() {

        System.out.println(redisTemplate.getKeySerializer());
        redisTemplate.setKeySerializer(redisTemplate.getStringSerializer());
        redisTemplate.setValueSerializer(redisTemplate.getStringSerializer());
        redisTemplate.opsForValue().set("abc","123");
        Assert.assertEquals("123",redisTemplate.opsForValue().get("abc"));
        Integer len = redisTemplate.opsForValue().append("abc", "456");
        Assert.assertEquals("6",len+"");
        Assert.assertEquals("123456",redisTemplate.opsForValue().get("abc"));
        Assert.assertEquals("12345",redisTemplate.opsForValue().get("abc",0,4));
        Assert.assertEquals("123456",redisTemplate.opsForValue().getAndSet("abc","123"));
        Assert.assertEquals("123",redisTemplate.opsForValue().get("abc"));
        Assert.assertEquals(Long.valueOf(124),redisTemplate.opsForValue().increment("abc", Long.valueOf(1)));

        boolean result = redisTemplate.opsForValue().setIfAbsent("abc", "abcdefg");
        boolean result2 = redisTemplate.opsForValue().setIfAbsent("aa", "abcdefg");
        System.out.println("result:"+result);
        System.out.println("result2:"+result2);
        Assert.assertEquals("124",redisTemplate.opsForValue().get("abc"));
        Assert.assertEquals("abcdefg",redisTemplate.opsForValue().get("aa"));

        Map<String, String> map = new HashMap<>();
        ArrayList<Object> keys = new ArrayList<>();
        for(int i=0;i<10;i++) {
            map.put("key" + i, new User(i,"zhxyzhxy-"+i,i).toString());
            keys.add("key" + i);
        }
        redisTemplate.opsForValue().multiSet(map);
        List<Object> values = redisTemplate.opsForValue().multiGet(keys);
        for(Object obj : values) {
            System.out.println(JSON.toJSONString(obj));
        }
        map = new HashMap<>();
        ArrayList<Object> newKeys = new ArrayList<>();
        for(int i=0;i<15;i++) {
            map.put("key" + i, i + "-");
            newKeys.add("key" + i);
        }
        boolean result3 = redisTemplate.opsForValue().multiSetIfAbsent(map);
        System.out.println("***************"+result3);
        values = redisTemplate.opsForValue().multiGet(newKeys);
        for(Object obj : values) {
            System.out.println(JSON.toJSONString(obj));
        }
        Assert.assertEquals(Long.valueOf(7),redisTemplate.opsForValue().size("aa"));
    }


    @Test
    public void testOpsForHash(){
        redisTemplate.setKeySerializer(redisTemplate.getStringSerializer());
        redisTemplate.setHashKeySerializer(redisTemplate.getStringSerializer());
        redisTemplate.setHashValueSerializer(redisTemplate.getStringSerializer());
        redisTemplate.opsForHash().put("user","name","张晓燕");
        redisTemplate.opsForHash().put("user","age","18");
        Assert.assertEquals("张晓燕",redisTemplate.opsForHash().get("user","name"));
        Assert.assertEquals("18",redisTemplate.opsForHash().get("user","age"));
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put("name", "zhxy");
        valueMap.put("age", "20");
        redisTemplate.opsForHash().putAll("user2",valueMap);
        Assert.assertEquals("zhxy",redisTemplate.opsForHash().get("user2","name"));
        Assert.assertEquals("20",redisTemplate.opsForHash().get("user2","age"));
        redisTemplate.opsForHash().putIfAbsent("user2", "age","21");
        redisTemplate.opsForHash().putIfAbsent("user2", "school","school123");
        Assert.assertEquals("20",redisTemplate.opsForHash().get("user2","age"));
        Assert.assertEquals("school123",redisTemplate.opsForHash().get("user2","school"));
        Assert.assertEquals(true,redisTemplate.opsForHash().hasKey("user2","school"));
        Map<Object, Object> entryMap = redisTemplate.opsForHash().entries("user2");
        for(Map.Entry entry : entryMap.entrySet()) {
            System.out.println(JSON.toJSONString(entry.getKey()) + "\t" + JSON.toJSONString(entry.getValue()));
        }
        System.out.println("------------");
        List<Object> keys = new ArrayList<>();
        keys.add("name");
        keys.add("age");
        List<Object> list = redisTemplate.opsForHash().multiGet("user2", keys);
        for(Object obj : list) {
            System.out.println(JSON.toJSONString(obj));
        }
        System.out.println("---------");
        Set<Object> keySet = redisTemplate.opsForHash().keys("user2");
        for(Object obj : keySet) {
            System.out.println(JSON.toJSONString(obj));
        }
        redisTemplate.opsForHash().increment("user2", "age", Long.valueOf(3));
        Assert.assertEquals("23",redisTemplate.opsForHash().get("user2","age"));

        System.out.println("----------------");
        Cursor<Map.Entry<Object, Object>> cursor = redisTemplate.opsForHash().scan("user2", null);
        while (cursor.hasNext()) {
            Map.Entry<Object, Object> entry = cursor.next();
            System.out.println(entry.getKey()+"\t"+entry.getValue());
        }
    }

    @Test
    public void testType() {
        redisTemplate.setKeySerializer(redisTemplate.getStringSerializer());
        redisTemplate.setValueSerializer(redisTemplate.getStringSerializer());
        Jackson2JsonRedisSerializer jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer<User>(User.class);
        redisTemplate.setValueSerializer(jackson2JsonRedisSerializer);

        List<User> list = new ArrayList<>();
        for(int i=10;i<15;i++) {
            list.add(new User(i, "zhxy" + i, i));
        }
        User user1 =  new User(1, "zhxy", 18);
        User user2 =  new User(2, "zhxy2", 19);
        User user3 =  new User(3, "zhxy2", 20);
        List<List<User>> list1 = Arrays.asList(list);
        List<User> list2 = Arrays.asList(user1, user2, user3);

        System.out.println(list2);
        ArrayList<User> list3 = new ArrayList<>();
        User[] arr = {user1,user2,user3};
        list3.add(user1);
        list3.add(user2);
        list3.add(user3);

        System.out.println(list1.getClass());
        System.out.println(list2.getClass());
        System.out.println(list3.getClass());
        System.out.println(list2.getClass().equals(Arrays.asList(user1,user2,user3).getClass()));
        redisTemplate.opsForList().leftPushAll("list3", list2);
        redisTemplate.opsForList().leftPushAll("list4", arr);
        redisTemplate.opsForList().leftPushAll("list4", list3);
        redisTemplate.opsForList().leftPushAll("list2", Arrays.asList(user1, user2, user3));


//        redisTemplate.opsForList().leftPushAll("list1", Arrays.asList(list));
    }

    @Test
    public void testOpsForList() {
        redisTemplate.setKeySerializer(redisTemplate.getStringSerializer());
        redisTemplate.setValueSerializer(redisTemplate.getStringSerializer());
        Jackson2JsonRedisSerializer jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer<User>(User.class);
        redisTemplate.setValueSerializer(jackson2JsonRedisSerializer);
        User user1 =  new User(1, "zhxy", 18);
        User user2 =  new User(2, "zhxy2", 19);
        User user3 =  new User(3, "zhxy2", 20);
        User user4 =  new User(4, "zhxy2", 20);

        redisTemplate.opsForList().leftPush("list1",user1);
        redisTemplate.opsForList().leftPush("list1", user2);
//
        User user = (User) redisTemplate.opsForList().leftPop("list1");
        System.out.println(JSON.toJSONString(user));
        user = (User) redisTemplate.opsForList().leftPop("list1");
        System.out.println(JSON.toJSONString(user));

        redisTemplate.opsForList().leftPush("list1", user1, user3);
        User[] users = new User[5];

        List<User> list = new ArrayList<>();
        for(int i=0;i<5;i++) {
            User u = new User(i, "zhxy" + i, i);
            list.add(u);
            users[i] = u;
        }
        redisTemplate.opsForList().leftPushAll("list1", Arrays.asList(user1,user2,user3));
        redisTemplate.opsForList().leftPushAll("list1", Arrays.asList(user3,user2,user1));
        redisTemplate.opsForList().leftPushIfPresent("list1", user4);
        redisTemplate.opsForList().leftPushIfPresent("list2", user3); //如果key存在，则插入
        List<Object> valueList = redisTemplate.opsForList().range("list1", 0, 5);
        System.out.println(valueList);
        redisTemplate.opsForList().remove("list1", 0, user1);
        redisTemplate.opsForList().remove("list1", 0, user2);
        redisTemplate.opsForList().remove("list1", 1, user3);
//        redisTemplate.opsForList().remove("list1", -1, user4);
//        redisTemplate.opsForList().remove("list1", 0, user3);
//        redisTemplate.opsForList().remove("list1", 0, user4);
        redisTemplate.opsForList().trim("list1",0,2);

//        redisTemplate.opsForList().leftPushAll("list1", list.get(0),list.get(1),list.get(2));
    }

    @Test
    public void testOpsForSet(){
        redisTemplate.setKeySerializer(redisTemplate.getStringSerializer());
        redisTemplate.setValueSerializer(redisTemplate.getStringSerializer());
        Jackson2JsonRedisSerializer jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer<User>(User.class);
        redisTemplate.setValueSerializer(jackson2JsonRedisSerializer);
        User user1 =  new User(1, "zhxy", 18);
        User user2 =  new User(2, "zhxy2", 19);
        User user3 =  new User(3, "zhxy2", 20);
        User user4 =  new User(4, "zhxy2", 20);

        redisTemplate.opsForSet().add("set1", user1, user1, user2);
        redisTemplate.opsForSet().add("set2", user1, user1, user2,user3,user4);
        redisTemplate.opsForSet().add("set4", user1,user4);
        Set<Object> difference = redisTemplate.opsForSet().difference("set1", "set2");
        Set<Object> difference2 = redisTemplate.opsForSet().difference("set2", "set1");
        System.out.println(difference);
        System.out.println(difference2);
        redisTemplate.opsForSet().differenceAndStore("set2", "set1", "set3");
        Set<Object> distinct = redisTemplate.opsForSet().distinctRandomMembers("set2", 2); //随机获取不同的对象
        System.out.println(distinct);
        Set<Object> intersect = redisTemplate.opsForSet().intersect("set1", "set2");
        System.out.println(intersect);
        intersect = redisTemplate.opsForSet().intersect("set1", Arrays.asList("set2", "set4"));
        System.out.println(intersect);
        redisTemplate.opsForSet().intersectAndStore("set1","set2","set12intersect");
        redisTemplate.opsForSet().intersectAndStore("set1",Arrays.asList("set2","set4"),"set124intersect");
        Assert.assertEquals(true, redisTemplate.opsForSet().isMember("set1", user1));
        Set<Object> members = redisTemplate.opsForSet().members("set1");
        System.out.println(members);
        redisTemplate.opsForSet().move("set2", user4, "set1");
        Object obj = redisTemplate.opsForSet().pop("set1");
        System.out.println(obj);
        obj = redisTemplate.opsForSet().randomMember("set1");
        System.out.println(obj);
        List<Object> list = redisTemplate.opsForSet().randomMembers("set1", 3);
        System.out.println(list);
        redisTemplate.opsForSet().remove("set1", user4);
        Cursor<Object> cursor = redisTemplate.opsForSet().scan("set2", null);
        while(cursor.hasNext()) {
            Object o = cursor.next();
            System.out.println(o);
        }
        Assert.assertEquals(Long.valueOf(3),redisTemplate.opsForSet().size("set2"));

        Set<Object> unionSet = redisTemplate.opsForSet().union("set1", "set2");
        System.out.println(unionSet);
        unionSet = redisTemplate.opsForSet().union("set1", Arrays.asList("set2", "set3"));
        System.out.println(unionSet);
        redisTemplate.opsForSet().unionAndStore("set1", "set2", "setunion12");
        redisTemplate.opsForSet().unionAndStore("set1", Arrays.asList("set2","set4"), "setunion124");

    }

    @Test
    public void testOpsForZSet() {
        redisTemplate.setKeySerializer(redisTemplate.getStringSerializer());
        redisTemplate.setValueSerializer(redisTemplate.getStringSerializer());
        Jackson2JsonRedisSerializer jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer<User>(User.class);
        redisTemplate.setValueSerializer(jackson2JsonRedisSerializer);
        User user1 =  new User(1, "zhxy", 18);
        User user2 =  new User(2, "zhxy2", 19);
        User user3 =  new User(3, "zhxy2", 20);
        User user4 =  new User(4, "zhxy2", 20);

        redisTemplate.opsForZSet().add("zset1", user1, 1);
        redisTemplate.opsForZSet().add("zset1", user2, 2);
        redisTemplate.opsForZSet().add("zset1", user2, 3);
        Assert.assertEquals(Long.valueOf(1),redisTemplate.opsForZSet().count("zset1", 0, 2));
        redisTemplate.opsForZSet().incrementScore("zset1", user2, 3);

        redisTemplate.opsForZSet().add("zset2", user1, 1);
        redisTemplate.opsForZSet().add("zset2", user2, 2);
        redisTemplate.opsForZSet().add("zset2", user3, 3);

        redisTemplate.opsForZSet().add("zset3", user1, 1);

        redisTemplate.opsForZSet().intersectAndStore("zset1", "zset2", "zsetinter12");
        redisTemplate.opsForZSet().intersectAndStore("zset1", Arrays.asList("zset2","zset3"), "zsetinter123");

        Set<Object> objectSet = redisTemplate.opsForZSet().range("zset1", 0, 2);
        System.out.println(objectSet);

        objectSet = redisTemplate.opsForZSet().rangeByScore("zset1", 0, 1);
        System.out.println(objectSet);

        System.out.println(redisTemplate.opsForZSet().rangeByScore("zset1",0,5,1,1));

        Set<ZSetOperations.TypedTuple<Object>> withScoreSet = redisTemplate.opsForZSet().rangeByScoreWithScores("zset1", 0, 6);
        for(ZSetOperations.TypedTuple<Object> obj : withScoreSet){
            System.out.println(obj.getValue()+"\t"+obj.getScore());
        }

        System.out.println(redisTemplate.opsForZSet().rank("zset1", user2));

        System.out.println(redisTemplate.opsForZSet().reverseRange("zset1",0,1));

        Cursor cursor = redisTemplate.opsForZSet().scan("zset2", null);
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }

        System.out.println(redisTemplate.opsForZSet().score("zset1",user2));
        System.out.println(redisTemplate.opsForZSet().size("zset1"));
        System.out.println(redisTemplate.opsForZSet().zCard("zset1"));
        redisTemplate.opsForZSet().unionAndStore("zset1", "zset2", "zsetunion12"); //score 会相加
        redisTemplate.opsForZSet().unionAndStore("zset1", Arrays.asList("zset2", "zset3"), "zsetunion123");

        System.out.println(redisTemplate.opsForZSet().remove("zset1",user1,user2));
        System.out.println(redisTemplate.opsForZSet().removeRange("zset2",0,1));
        System.out.println(redisTemplate.opsForZSet().removeRangeByScore("zsetunion12",0,3));


    }

    @Test
    public void testUser() {
            List<User> list = new ArrayList<>();
            List<String> keys = new ArrayList<>();
            for(int i=0; i<100;i++) {
            User user = new User(i, "zhxy-" + i, i);
            keys.add("zhxy-" + i);
            list.add(user);
        }
        long start = System.currentTimeMillis();
        redisTemplate.execute(new RedisCallback<Object>() {
            @Override
            public Object doInRedis(RedisConnection redisConnection) throws DataAccessException {
                redisConnection.flushDb();
                RedisSerializer<String> stringSerializer = redisTemplate.getStringSerializer();
                RedisSerializer<User> jdkSerializationRedisSerializer = (RedisSerializer<User>) redisTemplate.getValueSerializer();
                for (User user : list) {
                    redisConnection.set(stringSerializer.serialize(user.getName()),jdkSerializationRedisSerializer.serialize(user));
                }
                return null;
            }
        });



        User user = redisTemplate.execute(new RedisCallback<User>() {
            @Override
            public User doInRedis(RedisConnection redisConnection) throws DataAccessException {
                RedisSerializer<String> stringSerializer = redisTemplate.getStringSerializer();
                RedisSerializer<User> jdkSerializationRedisSerializer = (RedisSerializer<User>) redisTemplate.getValueSerializer();
                for(int i=0;i<100;i++) {
                    User user = jdkSerializationRedisSerializer.deserialize(redisConnection.get(stringSerializer.serialize("zhxy-"+ i)));
                    System.out.println(JSON.toJSONString(user));
                }

                User user = jdkSerializationRedisSerializer.deserialize(redisConnection.get(stringSerializer.serialize("zhxy-1")));
                return user;
            }
        });
        System.out.println("*** user : "+JSON.toJSONString(user));

        long end = System.currentTimeMillis();
        System.out.println("without pipeline user " + (end - start) +"ms");
    }

    @Test
    public void testUserUsePipeline() {
        List<User> list = new ArrayList<>();
        for(int i=0;i<100;i++) {
            User user = new User(i, "zhxy-" + i, i);
            list.add(user);
        }
        long start = System.currentTimeMillis();


        redisTemplate.executePipelined(new RedisCallback<Object>() {
            @Override
            public Object doInRedis(RedisConnection redisConnection) throws DataAccessException {
                redisConnection.flushDb();
                RedisSerializer<String> stringSerializer = redisTemplate.getStringSerializer();
                RedisSerializer<User> jdkSerializationRedisSerializer = (RedisSerializer<User>) redisTemplate.getValueSerializer();
                for (User user : list) {
                    redisConnection.rPush(stringSerializer.serialize("userlist"), jdkSerializationRedisSerializer.serialize(user));
                }
                return null;
            }
        });

//        List<Object> userList = redisTemplate.opsForList().range(redisTemplate.getStringSerializer().serialize("userlist"), 0, 100);

        List<Object> userList =  redisTemplate.executePipelined(new RedisCallback<Object>() {
                @Override
                public Object doInRedis(RedisConnection redisConnection) throws DataAccessException {
                    RedisSerializer<String> stringSerializer = redisTemplate.getStringSerializer();
                    for(int i=0;i<100;i++) {
                        redisConnection.lPop(stringSerializer.serialize("userlist"));
                    }
                    return null;
            }
        });
        for (Object user : userList) {
            System.out.println(JSON.toJSONString(user));
        }
        long end = System.currentTimeMillis();
        System.out.println("with pipeline user " + (end - start) +"ms");
    }

}
