package com.qiniu.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
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

    public static String getCurrentDatetime() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(new Date());
    }
}
