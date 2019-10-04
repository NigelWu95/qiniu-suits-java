package com.qiniu.util;

import org.junit.Test;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

public class DatetimeUtilsTest {

    @Test
    public void newDateTimeApi() {
        System.out.println(DatetimeUtils.datetimeOf(15319680669903952L));
        System.out.println(DatetimeUtils.stringOf(0, 10000000));
        System.out.println(LocalDateTime.now());
    }

    @Test
    public void test() {
        Map<String, String> map = new HashMap<String, String>(){{
//            put("timestamp", "1519541435");
//            put("timestamp", "15195414357425830");
//            put("datetime", "2018-02-25T14:50:35.742583");
            put("lastModified", "2018-02-25T14:50:35.742583");
        }};
        LocalDateTime localDateTime;
        String datetime = map.get(ConvertingUtils.defaultDatetimeField);
        if (datetime == null) {
            String timestamp = map.get("timestamp");
            if (timestamp == null) {
                timestamp = map.get("putTime");
            }
            if (timestamp == null) {
                datetime = map.get("lastModified");
            } else {
                long accuracy = (long) Math.pow(10, (timestamp.length() - 10));
                datetime = DatetimeUtils.stringOf(Long.valueOf(timestamp), accuracy);
            }
        }
        localDateTime = LocalDateTime.parse(datetime);
        System.out.println(localDateTime);
    }
}