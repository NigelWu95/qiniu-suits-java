package com.qiniu.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
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
//        return sdf_YYYY_MMTdd_HH_MM_SS_SSS.format(date);
        return stringOf(date.toInstant()); // format(date) 方法可能会抛数组越界异常
    }

    public static String stringOf(Date date, SimpleDateFormat sdf) {
        if (sdf == null) return sdf_YYYY_MMTdd_HH_MM_SS_SSS.format(date);
        else return sdf.format(date);
    }

    // java8 time api

    public static String stringOf(long epochSecond) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSecond), defaultZoneId).toString();
    }

    public static String stringOf(Instant instant) {
        return LocalDateTime.ofInstant(instant, defaultZoneId).toString();
    }

    /**
     * 将 timeStamp 根据精确度转换为 dateTimeFormatter 格式的时间日期字符串
     * @param timestamp 时间戳
     * @param accuracy 秒数小数点之后精确度，纳秒倍数，如百纳秒时间戳精确度为 10_000_000L
     * @return 返回格式化的日期字符串
     */
    public static String stringOf(long timestamp, long accuracy) {
        return datetimeOf(timestamp, accuracy).toString();
    }

    /**
     * 将 timeStamp 根据精确度转换为 "yyyy-MM-DD'T'HH:MM:SS.SSS" 格式的时间日期字符串
     * @param timestamp 时间戳
     * @param accuracy 秒数小数点之后精确度，纳秒倍数，如百纳秒时间戳精确度为 10_000_000L
     * @param dateTimeFormatter 时间日期格式
     * @return 返回格式化的日期字符串
     */
    public static String stringOf(long timestamp, long accuracy, DateTimeFormatter dateTimeFormatter) {
        if (dateTimeFormatter == null) {
            return stringOf(timestamp, accuracy);
        } else {
            long ratio = 1000_000_000L / accuracy;
            return dateTimeFormatter.format(LocalDateTime.ofInstant(Instant.ofEpochSecond(Math.floorDiv(timestamp, accuracy),
                    Math.floorMod(timestamp, accuracy) * ratio), defaultZoneId));
        }
    }

    public static LocalDateTime datetimeOf(long timestamp, long accuracy) {
        long ratio = 1000_000_000L / accuracy;
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(Math.floorDiv(timestamp, accuracy),
                Math.floorMod(timestamp, accuracy) * ratio), defaultZoneId);
    }

    // 注意：该方法根据时间戳的长度自动判断精度，如 10 位的时间戳精度为秒，13 位的时间戳精度位毫秒，19 位的时间戳精度为纳秒
    public static LocalDateTime datetimeOf(long timestamp) {
        return datetimeOf(timestamp, (long)Math.pow(10, Math.floorMod(String.valueOf(timestamp).length(), 10)));
    }

    public final static Clock clock_Default = Clock.systemDefaultZone();
    public final static Clock clock_GMT = Clock.system(ZoneId.of("GMT"));
    /**
     * 获取 GMT 格式时间戳，'Tue, 3 Jun 2008 11:05:30 GMT'
     * DateTimeFormatter.RFC_1123_DATE_TIME
     * @return GMT 格式时间戳
     */
    public static String getGMTDate() {
        SimpleDateFormat formatter = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US);
        formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
//        return formatter.format(new Date());
        return formatter.format(new Date(Instant.now(clock_GMT).toEpochMilli()));
    }
}
