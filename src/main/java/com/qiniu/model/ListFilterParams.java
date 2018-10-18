package com.qiniu.model;

import com.qiniu.util.DateUtils;
import com.qiniu.util.StringUtils;

import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

public class ListFilterParams extends BaseParams {

    private String keyPrefix;
    private String keySuffix;
    private String keyRegex;
    private String pointDate;
    private String pointTime;
    private String direction;
    private String mime;
    private String type;

    public ListFilterParams(String[] args) throws Exception {
        super(args);
        try { this.keyPrefix = getParamFromArgs("f-key-prefix"); } catch (Exception e) {}
        try { this.keySuffix = getParamFromArgs("f-key-suffix"); } catch (Exception e) {}
        try { this.keyRegex = getParamFromArgs("f-key-regex"); } catch (Exception e) {}
        try { this.pointDate = getParamFromArgs("f-date"); } catch (Exception e) {}
        try { this.pointTime = getParamFromArgs("f-time"); } catch (Exception e) {}
        try { this.direction = getParamFromArgs("f-direction"); } catch (Exception e) {}
        try { this.mime = getParamFromArgs("f-mime"); } catch (Exception e) {}
        try { this.type = getParamFromArgs("f-type"); } catch (Exception e) {}
    }

    public ListFilterParams(String configFileName) throws Exception {
        super(configFileName);
        try { this.keyPrefix = getParamFromConfig("f-key-prefix"); } catch (Exception e) {}
        try { this.keySuffix = getParamFromConfig("f-key-suffix"); } catch (Exception e) {}
        try { this.keyRegex = getParamFromConfig("f-key-regex"); } catch (Exception e) {}
        try { this.pointDate = getParamFromConfig("f-date"); } catch (Exception e) {}
        try { this.pointTime = getParamFromConfig("f-time"); } catch (Exception e) {}
        try { this.direction = getParamFromConfig("f-direction"); } catch (Exception e) {}
        try { this.mime = getParamFromConfig("f-mime"); } catch (Exception e) {}
        try { this.type = getParamFromConfig("f-type"); } catch (Exception e) {}
    }

    public List<String> getKeyPrefix() {
        return Arrays.asList(keyPrefix.split(","));
    }

    public List<String> getKeySuffix() {

        return Arrays.asList(keySuffix.split(","));
    }

    public List<String> getKeyRegex() {
        return Arrays.asList(keyRegex.split(","));
    }

    private Long getPointDatetime() throws ParseException {

        String pointDatetime;

        if (StringUtils.isNullOrEmpty(pointDate) && StringUtils.isNullOrEmpty(pointDate)) {
            System.out.println("datetime is empty, it will not compare by put time.");
            return 0L;
        } else if(pointDate.matches("\\d{4}-\\d{2}-\\d{2}") && pointTime.matches("\\d{2}:\\d{2}:\\d{2}")) {
            pointDatetime =  pointDate + " " + pointTime;
            return DateUtils.parseYYYYMMDDHHMMSSdatetime(pointDatetime);
        } else {
            System.out.println("datetime is empty, it will not compare by put time.");
            return 0L;
        }

    }

    private boolean getDirection() {
        if (StringUtils.isNullOrEmpty(direction) || !direction.matches("\\d")) {
            System.out.println("no incorrect direction, it will use 0(pre) as default. But it will not take effect if datetime is empty.");
            return true;
        } else {
            return Integer.valueOf(direction) == 0;
        }
    }

    public long getPutTimeMax() throws ParseException {
        if (getDirection()) return getPointDatetime() * 10000000;
        return 0;
    }

    public long getPutTimeMin() throws ParseException {
        if (!getDirection()) return getPointDatetime() * 10000000;
        return 0;
    }

    public List<String> getMime() {
        return Arrays.asList(mime.split(","));
    }

    public int getType() throws Exception {
        if (type.matches("(0|1)")) {
            return Integer.valueOf(type);
        } else {
            throw new Exception("the direction is incorrect, please set it 0 or 1");
        }
    }
}