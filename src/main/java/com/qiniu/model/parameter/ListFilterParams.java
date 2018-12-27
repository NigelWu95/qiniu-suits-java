package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;
import com.qiniu.util.DateUtils;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

public class ListFilterParams extends CommonParams {

    private String keyPrefix;
    private String keySuffix;
    private String keyRegex;
    private String pointDate;
    private String pointTime;
    private String direction;
    private String mime;
    private String type;
    private long datetime;
    private boolean directionFlag;
    private String antiKeyPrefix;
    private String antiKeySuffix;
    private String antiKeyRegex;
    private String antiMime;

    public ListFilterParams(IEntryParam entryParam) throws Exception {
        super(entryParam);
        try { this.keyPrefix = entryParam.getParamValue("f-key-prefix"); } catch (Exception e) {}
        try { this.keySuffix = entryParam.getParamValue("f-key-suffix"); } catch (Exception e) {}
        try { this.keyRegex = entryParam.getParamValue("f-key-regex"); } catch (Exception e) {}
        try { this.pointDate = entryParam.getParamValue("f-date"); } catch (Exception e) { pointDate = ""; }
        try { this.pointTime = entryParam.getParamValue("f-time"); } catch (Exception e) { pointTime = ""; }
        try { this.direction = entryParam.getParamValue("f-direction"); } catch (Exception e) { direction = ""; }
        try { this.mime = entryParam.getParamValue("f-mime"); } catch (Exception e) {}
        try { this.type = entryParam.getParamValue("f-type"); } catch (Exception e) { type = ""; }
        this.datetime = getPointDatetime();
        if (!"".equals(pointDate)) this.directionFlag = getDirection();
        try { this.antiKeyPrefix = entryParam.getParamValue("anti-f-key-prefix"); } catch (Exception e) {}
        try { this.antiKeySuffix = entryParam.getParamValue("anti-f-key-suffix"); } catch (Exception e) {}
        try { this.antiKeyRegex = entryParam.getParamValue("anti-f-key-regex"); } catch (Exception e) {}
        try { this.antiMime = entryParam.getParamValue("anti-f-mime"); } catch (Exception e) {}
    }

    public List<String> getKeyPrefix() {
        if (keyPrefix != null && !"".equals(keyPrefix)) return Arrays.asList(keyPrefix.split(","));
        else return null;
    }

    public List<String> getKeySuffix() {
        if (keySuffix != null && !"".equals(keySuffix)) return Arrays.asList(keySuffix.split(","));
        else return null;
    }

    public List<String> getKeyRegex() {
        if (keyRegex != null && !"".equals(keyRegex)) return Arrays.asList(keyRegex.split(","));
        else return null;
    }

    private Long getPointDatetime() throws ParseException {
        String pointDatetime;
        if(pointDate.matches("\\d{4}-\\d{2}-\\d{2}")) {
            if (pointTime.matches("\\d{2}:\\d{2}:\\d{2}"))
                pointDatetime =  pointDate + " " + pointTime;
            else {
                pointDatetime =  pointDate + " " + "00:00:00";
            }
            return DateUtils.parseYYYYMMDDHHMMSSdatetime(pointDatetime);
        } else {
            return 0L;
        }

    }

    private boolean getDirection() throws IOException {
        if (direction.matches("\\d")) {
            return Integer.valueOf(direction) == 0;
        } else {
            throw new IOException("no incorrect direction, please set it 0/1.");
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
        if (mime != null && !"".equals(mime)) return Arrays.asList(mime.split(","));
        else return null;
    }

    public int getType() {
        if (type.matches("([01])")) {
            return Integer.valueOf(type);
        } else {
            return -1;
        }
    }

    public List<String> getAntiKeyPrefix() {
        if (antiKeyPrefix != null && !"".equals(antiKeyPrefix)) return Arrays.asList(antiKeyPrefix.split(","));
        else return null;
    }

    public List<String> getAntiKeySuffix() {
        if (antiKeySuffix != null && !"".equals(antiKeySuffix)) return Arrays.asList(antiKeySuffix.split(","));
        else return null;
    }

    public List<String> getAntiKeyRegex() {
        if (antiKeyRegex != null && !"".equals(antiKeyRegex)) return Arrays.asList(antiKeyRegex.split(","));
        else return null;
    }

    public List<String> getAntiMime() {
        if (antiMime != null && !"".equals(antiMime)) return Arrays.asList(antiMime.split(","));
        else return null;
    }
}
