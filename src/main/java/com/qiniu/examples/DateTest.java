package com.qiniu.examples;

import com.qiniu.util.DateUtils;

import java.text.ParseException;

public class DateTest {

    public static void main(String[] args) {

        try {
            System.out.println(DateUtils.parseDateToStamp("2018-08-09T00:00:00+08:00"));
            System.out.println(DateUtils.parseDateToStamp("2018-08-09T00:00:00.000+08:00"));
            System.out.println(DateUtils.parseDateToStamp("2018-08-09 00:00:00+08:00"));
            System.out.println(DateUtils.parseDateToStamp("2018-08-09 00:00:00.000+08:00"));
            System.out.println(DateUtils.parseDateToStamp("2018-08-09T00:00:00Z"));
            System.out.println(DateUtils.parseDateToStamp("2018-08-09T00:00:00.000Z"));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}