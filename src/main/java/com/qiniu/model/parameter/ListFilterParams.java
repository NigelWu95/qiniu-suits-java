package com.qiniu.model.parameter;

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
    private String direction = "";
    private String mime;
    private String type = "";
    private long datetime;
    private boolean directionFlag;
    private String antiKeyPrefix;
    private String antiKeySuffix;
    private String antiKeyRegex;
    private String antiMime;

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
        this.datetime = getPointDatetime();
        this.directionFlag = getDirection();
        try { this.antiKeyPrefix = getParamFromArgs("anti-f-key-prefix"); } catch (Exception e) {}
        try { this.antiKeySuffix = getParamFromArgs("anti-f-key-suffix"); } catch (Exception e) {}
        try { this.antiKeyRegex = getParamFromArgs("anti-f-key-regex"); } catch (Exception e) {}
        try { this.antiMime = getParamFromArgs("anti-f-mime"); } catch (Exception e) {}
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
        this.datetime = getPointDatetime();
        this.directionFlag = getDirection();
        try { this.antiKeyPrefix = getParamFromConfig("anti-f-key-prefix"); } catch (Exception e) {}
        try { this.antiKeySuffix = getParamFromConfig("anti-f-key-suffix"); } catch (Exception e) {}
        try { this.antiKeyRegex = getParamFromConfig("anti-f-key-regex"); } catch (Exception e) {}
        try { this.antiMime = getParamFromConfig("anti-f-mime"); } catch (Exception e) {}
    }

    public List<String> getKeyPrefix() {
        if (StringUtils.isNullOrEmpty(keyPrefix)) return null;
        return Arrays.asList(keyPrefix.split(","));
    }

    public List<String> getKeySuffix() {
        if (StringUtils.isNullOrEmpty(keySuffix)) return null;
        return Arrays.asList(keySuffix.split(","));
    }

    public List<String> getKeyRegex() {
        if (StringUtils.isNullOrEmpty(keyRegex)) return null;
        return Arrays.asList(keyRegex.split(","));
    }

    private Long getPointDatetime() throws ParseException {

        String pointDatetime;

        if (StringUtils.isNullOrEmpty(pointDate) && StringUtils.isNullOrEmpty(pointTime)) {
            System.out.println("datetime is empty, it will not compare to the put time.");
            return 0L;
        } else if(pointDate.matches("\\d{4}-\\d{2}-\\d{2}")) {
            if (pointTime.matches("\\d{2}:\\d{2}:\\d{2}"))
                pointDatetime =  pointDate + " " + pointTime;
            else {
                System.out.println("time is empty, it will use 00:00:00 as default.");
                pointDatetime =  pointDate + " " + "00:00:00";
            }
            return DateUtils.parseYYYYMMDDHHMMSSdatetime(pointDatetime);
        } else {
            System.out.println("datetime is empty, it will not compare to the put time.");
            return 0L;
        }

    }

    private boolean getDirection() {
        if (direction.matches("\\d")) {
            return Integer.valueOf(direction) == 0;
        } else {
            System.out.println("no incorrect direction, it will use 0(pre) as default. But it will not take effect if" +
                    " datetime is empty.");
            return true;
        }
    }

    public long getPutTimeMax() {
        if (directionFlag) return datetime * 10000;
        return 0;
    }

    public long getPutTimeMin() {
        if (!directionFlag) return datetime * 10000;
        return 0;
    }

    public List<String> getMime() {
        if (StringUtils.isNullOrEmpty(mime)) return null;
        return Arrays.asList(mime.split(","));
    }

    public int getType() {
        if (type.matches("(0|1)")) {
            return Integer.valueOf(type);
        } else {
            System.out.println("no incorrect type, it will use 0 as default");
            return -1;
        }
    }

    public List<String> getAntiKeyPrefix() {
        if (StringUtils.isNullOrEmpty(antiKeyPrefix)) return null;
        return Arrays.asList(antiKeyPrefix.split(","));
    }

    public List<String> getAntiKeySuffix() {
        if (StringUtils.isNullOrEmpty(antiKeySuffix)) return null;
        return Arrays.asList(antiKeySuffix.split(","));
    }

    public List<String> getAntiKeyRegex() {
        if (StringUtils.isNullOrEmpty(antiKeyRegex)) return null;
        return Arrays.asList(antiKeyRegex.split(","));
    }

    public List<String> getAntiMime() {
        if (StringUtils.isNullOrEmpty(antiMime)) return null;
        return Arrays.asList(antiMime.split(","));
    }
}
