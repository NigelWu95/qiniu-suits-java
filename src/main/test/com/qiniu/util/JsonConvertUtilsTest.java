package com.qiniu.util;

import org.junit.Test;

public class JsonConvertUtilsTest {

    @Test
    public void testToJson() {
        System.out.println(JsonConvertUtils.toJson("abc"));
        System.out.println(JsonConvertUtils.toJson(new StringBuilder("abc")));
    }
}