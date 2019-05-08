package com.qiniu.util;

import org.junit.Test;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;

public class DateUtilsTest {

    @Test
    public void timeStamp2Date() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSX");;
        long timeStamp = 1515730332000L;
        Date date = new Date(timeStamp);
        System.out.println(sdf.format(date));
        System.out.println(sdf.format(1515730332000d));
//        LocalDateTime
    }
}