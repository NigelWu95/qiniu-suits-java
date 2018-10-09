package com.qiniu.util;
import com.qiniu.common.QiniuSuitsException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DateUtils {

    public static boolean compareTimeToBreakpoint(String pointTime, boolean isBiggerThan, Long timeStamp) throws ParseException, QiniuSuitsException {
        if (StringUtils.isNullOrEmpty(pointTime)) {
            return true;
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Long breakpoint = sdf.parse(pointTime).getTime();
        return (breakpoint > timeStamp) == isBiggerThan;
    }

    public static boolean compareTimeToBreakpoint(String pointTime, boolean isBiggerThan, String timeString) throws ParseException, QiniuSuitsException {
        if (StringUtils.isNullOrEmpty(pointTime)) {
            return true;
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Long breakpoint = sdf.parse(pointTime).getTime();
        return (breakpoint > parseDateToStamp(timeString)) == isBiggerThan;
    }

    public static Long parseDateToStamp(String greenwichMeanTime) throws ParseException {

        SimpleDateFormat sd = new SimpleDateFormat();
        Long timeStamp = 0L;

        // yyyy-MM-dd'T'HH:mm:ssX（2018-08-09T11:38:05+08:00） 格式
        String pattern1 = "^[\\d]{4}-[\\d]{2}-[\\d]{2}T[\\d]{2}:[\\d]{2}:[\\d]{2}\\+[\\d]{2}:[\\d]{2}$";
        Pattern r1 = Pattern.compile(pattern1);
        Matcher m1 = r1.matcher(greenwichMeanTime);

        // yyyy-MM-dd'T'HH:mm:ss.SSSX（2018-08-09T11:38:05.333+08:00） 格式
        String pattern2 = "^[\\d]{4}-[\\d]{2}-[\\d]{2}T[\\d]{2}:[\\d]{2}:[\\d]{2}\\.[\\d]{1,3}\\+[\\d]{2}:[\\d]{2}$";
        Pattern r2 = Pattern.compile(pattern2);
        Matcher m2 = r2.matcher(greenwichMeanTime);

        if (m1.matches()) {
            sd = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
        } else if (m2.matches()) {
            sd = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
        } else {
            sd = new SimpleDateFormat(greenwichMeanTime.endsWith("+08:00") ?
                    greenwichMeanTime.length() == 25 ? "yyyy-MM-dd HH:mm:ssX" : "yyyy-MM-dd HH:mm:ss.SSSX" :
                    greenwichMeanTime.length() == 20 ? "yyyy-MM-dd'T'HH:mm:ss'Z'" : "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        }

        return timeStamp = sd.parse(greenwichMeanTime).getTime();
    }

    public static String[] getCurrentDatetime() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(new Date()).split(" ");
    }
}