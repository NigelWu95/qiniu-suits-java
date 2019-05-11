package com.qiniu.util;

import org.junit.Test;

import java.text.SimpleDateFormat;
import java.time.*;
import java.util.Date;

public class DateUtilsTest {

    @Test
    public void timeStamp2Date() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSX");
        long timeStamp = 1515730332000L;
        Date date = new Date(timeStamp);
        System.out.println(sdf.format(date));
        System.out.println(sdf.format(1515730332000d));
        System.out.println(LocalDateTime.parse("2018-01-01T12:12:12.999750909"));
        System.out.println(Instant.ofEpochSecond(10000));
        System.out.println(Instant.parse("2018-01-01T12:12:12.999Z").getNano());
        System.out.println(Instant.parse("2018-01-01T12:12:12.999Z").getEpochSecond());
        System.out.println(Instant.MAX);
        System.out.println(Instant.ofEpochSecond(1000, 1000000));
        System.out.println(Instant.ofEpochSecond(1000, 1000_000));
        System.out.println(Instant.ofEpochSecond(15319680669903952L));
        System.out.println(Instant.ofEpochSecond(0, 15319680669903952L));
        System.out.println(Instant.ofEpochSecond(0, 1531968066990395202L));
        System.out.println(LocalDateTime.ofEpochSecond(1515730332, 3952000, ZoneOffset.UTC));
        System.out.println(LocalTime.now().withNano(999999999));
        System.out.println(Duration.ofNanos(15319680669903952L).getNano());
        LocalDateTime localDateTime = LocalDateTime.now();
        System.out.println(localDateTime.withNano(3952000));
        System.out.println(Instant.now().toEpochMilli());
        System.out.println(Instant.now().getNano());
        System.out.println(Instant.ofEpochMilli(1557497974473333L));
        System.out.println(Long.MAX_VALUE);
    }
}