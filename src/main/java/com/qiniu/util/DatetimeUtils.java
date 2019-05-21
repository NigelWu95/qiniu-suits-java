package com.qiniu.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.regex.Pattern;

public final class DatetimeUtils {

    public final static SimpleDateFormat sdf_YYYY_MM_DD_HH_MM_SS_SSS = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    public final static SimpleDateFormat sdf_YYYY_MM_DD_HH_MM_SS_SSS_X = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSX");
    public final static SimpleDateFormat sdf_YYYY_MMTdd_HH_MM_SS_SSS = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    public final static SimpleDateFormat sdf_YYYY_MMTdd_HH_MM_SS_SSS_X = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
    // yyyy-MM-dd HH:mm:ss.SSS (2018-08-09T11:38:05.000) 格式
    public final static Pattern pattern_YYYY_MM_DD_HH_MM_SS_SSS = Pattern.compile(
            "^[\\d]{4}-[\\d]{2}-[\\d]{2} [\\d]{2}:[\\d]{2}:[\\d]{2}\\.[\\d]{1,3}$");
    // yyyy-MM-dd HH:mm:ss.SSSX (2018-08-09T11:38:05.000+08:00) 格式
    public final static Pattern pattern_YYYY_MM_DD_HH_MM_SS_SSS_X = Pattern.compile(
            "^[\\d]{4}-[\\d]{2}-[\\d]{2} [\\d]{2}:[\\d]{2}:[\\d]{2}\\.[\\d]{1,3}\\+[\\d]{2}(:[\\d]{2}|)$");
    // yyyy-MM-dd'T'HH:mm:ss.SSS (2018-08-09T11:38:05.333) 格式
    public final static Pattern pattern_YYYY_MM_DDTHH_MM_SS_SSS = Pattern.compile(
            "^[\\d]{4}-[\\d]{2}-[\\d]{2}T[\\d]{2}:[\\d]{2}:[\\d]{2}\\.[\\d]{1,3}$");
    // yyyy-MM-dd'T'HH:mm:ss.SSSX (2018-08-09T11:38:05.333+08:00) 格式
    public final static Pattern pattern_YYYY_MM_DDTHH_MM_SS_SSS_X = Pattern.compile(
            "^[\\d]{4}-[\\d]{2}-[\\d]{2}T[\\d]{2}:[\\d]{2}:[\\d]{2}\\.[\\d]{1,3}\\+[\\d]{2}(:[\\d]{2}|)$");

    // java8 time api
    public static ZoneId defaultZoneId = ZoneId.systemDefault();

    public static Date parseToDate(String dateTime) throws ParseException {
        if (pattern_YYYY_MM_DD_HH_MM_SS_SSS.matcher(dateTime).matches()) {
            return sdf_YYYY_MM_DD_HH_MM_SS_SSS.parse(dateTime);
        } else if (pattern_YYYY_MM_DD_HH_MM_SS_SSS_X.matcher(dateTime).matches()) {
            return sdf_YYYY_MM_DD_HH_MM_SS_SSS_X.parse(dateTime);
        } else if (pattern_YYYY_MM_DDTHH_MM_SS_SSS.matcher(dateTime).matches()) {
            return sdf_YYYY_MMTdd_HH_MM_SS_SSS.parse(dateTime);
        } else {
            return sdf_YYYY_MMTdd_HH_MM_SS_SSS_X.parse(dateTime);
        }
    }

    public static String stringOf(Date date) {
        return sdf_YYYY_MMTdd_HH_MM_SS_SSS.format(date);
    }

    public static String stringOf(Date date, SimpleDateFormat sdf) {
        if (sdf == null) return sdf_YYYY_MMTdd_HH_MM_SS_SSS.format(date);
        else return sdf.format(date);
    }

    // java8 time api

    public static String stringOf(long epochSecond) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSecond), defaultZoneId).toString();
    }

    public static String stringOf(long timeStamp, long accuracy) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(0,
                timeStamp * (1000000000L / accuracy)), defaultZoneId).toString();
    }

    public static LocalDateTime stringOf(String datetime) {
        return LocalDateTime.parse(datetime);
    }

    public static String stringOf(long timeStamp, long accuracy, DateTimeFormatter dateTimeFormatter) {
        if (dateTimeFormatter == null) {
            return stringOf(timeStamp, accuracy);
        } else {
            return dateTimeFormatter.format(LocalDateTime.ofInstant(Instant.ofEpochSecond(0,
                    timeStamp * 10^9 / accuracy), defaultZoneId));
        }
    }

    public static String stringOf(Instant instant) {
        return LocalDateTime.ofInstant(instant, defaultZoneId).toString();
    }
}
