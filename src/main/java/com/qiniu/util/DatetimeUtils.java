package com.qiniu.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DatetimeUtils {

    public final static SimpleDateFormat sdf_YYYY_MM_DD_HH_MM_SS = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public final static SimpleDateFormat sdf_YYYY_MMTdd_HH_MM_SS_X = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
    public final static SimpleDateFormat sdf_YYYY_MMTdd_HH_MM_SSS_X = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
    public final static SimpleDateFormat sdf_YYYY_MM_dd_HH_MM_SS = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//    public final static SimpleDateFormat sdf_YYYY_MM_dd_HH_MM_SS = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public String pattern_YYYY_MM_DDTHH_MM_SS_X = "^[\\d]{4}-[\\d]{2}-[\\d]{2}T[\\d]{2}:[\\d]{2}:[\\d]{2}\\+[\\d]{2}:[\\d]{2}$";

    public static boolean compareTimeToBreakpoint(String pointTime, boolean isBiggerThan, Long timeStamp)
            throws ParseException {
        if (StringUtils.isNullOrEmpty(pointTime)) {
            return true;
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Long breakpoint = sdf.parse(pointTime).getTime();
        return (breakpoint > timeStamp) == isBiggerThan;
    }

    public static boolean compareTimeToBreakpoint(String pointTime, boolean isBiggerThan, String timeString)
            throws ParseException {
        if (StringUtils.isNullOrEmpty(pointTime)) {
            return true;
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Long breakpoint = sdf.parse(pointTime).getTime();
        return (breakpoint > parseDateToStamp(timeString)) == isBiggerThan;
    }

    public static Long parseYYYYMMDDHHMMSSDatetime(String datetime) throws ParseException {
        SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sd.parse(datetime).getTime();
    }

    public static Long parseDateToStamp(String dateTime) throws ParseException {
        SimpleDateFormat sdf;
        // yyyy-MM-dd'T'HH:mm:ssX（2018-08-09T11:38:05+08:00） 格式
        String pattern1 = "^[\\d]{4}-[\\d]{2}-[\\d]{2}T[\\d]{2}:[\\d]{2}:[\\d]{2}\\+[\\d]{2}:[\\d]{2}$";
        Pattern r1 = Pattern.compile(pattern1);
        Matcher m1 = r1.matcher(dateTime);
        // yyyy-MM-dd'T'HH:mm:ss.SSSX（2018-08-09T11:38:05.333+08:00） 格式
        String pattern2 = "^[\\d]{4}-[\\d]{2}-[\\d]{2}T[\\d]{2}:[\\d]{2}:[\\d]{2}\\.[\\d]{1,3}\\+[\\d]{2}:[\\d]{2}$";
        Pattern r2 = Pattern.compile(pattern2);
        Matcher m2 = r2.matcher(dateTime);

        if (m1.matches()) {
            sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
        } else if (m2.matches()) {
            sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
        } else {
            sdf = new SimpleDateFormat(dateTime.endsWith("+08:00") || dateTime.endsWith("+08") ?
                    dateTime.length() == 25 ? "yyyy-MM-dd HH:mm:ssX" : "yyyy-MM-dd HH:mm:ss.SSSX" :
                    dateTime.length() == 20 ? "yyyy-MM-dd'T'HH:mm:ss'Z'" : "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        }

        return sdf.parse(dateTime).getTime();
    }

    public static Date parseToDate(String dateTime) throws ParseException {
        SimpleDateFormat sdf;
        // yyyy-MM-dd'T'HH:mm:ssX（2018-08-09T11:38:05+08:00） 格式
        String pattern1 = "^[\\d]{4}-[\\d]{2}-[\\d]{2}T[\\d]{2}:[\\d]{2}:[\\d]{2}\\+[\\d]{2}:[\\d]{2}$";
        Pattern r1 = Pattern.compile(pattern1);
        Matcher m1 = r1.matcher(dateTime);
        // yyyy-MM-dd'T'HH:mm:ss.SSSX（2018-08-09T11:38:05.333+08:00） 格式
        String pattern2 = "^[\\d]{4}-[\\d]{2}-[\\d]{2}T[\\d]{2}:[\\d]{2}:[\\d]{2}\\.[\\d]{1,3}\\+[\\d]{2}:[\\d]{2}$";
        Pattern r2 = Pattern.compile(pattern2);
        Matcher m2 = r2.matcher(dateTime);

        if (m1.matches()) {
            sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
        } else if (m2.matches()) {
            sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
        } else {
            sdf = new SimpleDateFormat(dateTime.endsWith("+08:00") || dateTime.endsWith("+08") ?
                    dateTime.length() == 25 ? "yyyy-MM-dd HH:mm:ssX" : "yyyy-MM-dd HH:mm:ss.SSSX" :
                    dateTime.length() == 20 ? "yyyy-MM-dd'T'HH:mm:ss'Z'" : "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        }

        return sdf.parse(dateTime);
    }

    public static String getCurrentDatetime() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(new Date());
    }

    public static String timeStamp2Date(String seconds,String format) {
        if(seconds == null || seconds.isEmpty() || seconds.equals("null")) {
            return "";
        }
        if(format == null || format.isEmpty()){
            format = "yyyy-MM-dd HH:mm:ss";
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(new Date(Long.valueOf(seconds+"000")));
    }
}
