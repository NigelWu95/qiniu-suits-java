package com.qiniu.util;

import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import org.junit.Test;

public class JsonUtilsTest {

    @Test
    public void testToJson() {
        System.out.println(JsonUtils.toJson("abc"));
        System.out.println(JsonUtils.toJson(new StringBuilder("abc")));
        System.out.println(JsonUtils.toJson(new String("abc")));
        System.out.println(JsonUtils.toString(null) == null);
        System.out.println(JsonUtils.toString(JsonNull.INSTANCE) == null);
//        System.out.println(JsonUtils.toString(new JsonObject()));
    }
}