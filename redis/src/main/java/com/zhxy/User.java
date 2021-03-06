package com.zhxy;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.HashMap;

/**
 * Created by zhxy on 1/3/2017.
 */
public class User implements Serializable {
    private int id;
    private String name;
    private int age;

    private String school;

    public User(){}

    public User(int id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getSchool() {
        return school;
    }

    public void setSchool(String school) {
        this.school = school;
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", school='" + school + '\'' +
                '}';
    }

    public HashMap<String,String> toMap(){
        HashMap<String,String> map = new HashMap<>();
        Field[] fields = this.getClass().getDeclaredFields();
        for(Field field : fields) {
            try {
                if(field.get(this) != null) {
                    map.put(field.getName().toString(), field.get(this).toString()); // 设置字段的名称为key，值为value
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return map;
    }
}
