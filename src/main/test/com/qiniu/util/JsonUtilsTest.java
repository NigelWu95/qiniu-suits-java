package com.qiniu.util;

import org.junit.Test;

public class JsonUtilsTest {

    @Test
    public void testToJson() {
        System.out.println(JsonUtils.toJson("abc"));
        System.out.println(JsonUtils.toJson(new StringBuilder("abc")));
        System.out.println(JsonUtils.toJson(new String("abc")));
    }
}