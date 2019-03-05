package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;
import com.qiniu.util.DateUtils;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

public class FileFilterParams extends CommonParams {

    private String keyPrefix;
    private String keySuffix;
    private String keyInner;
    private String keyRegex;
    private String pointDate;
    private String pointTime;
    private String direction;
    private String mimeType;
    private String type;
    private String status;
    private long datetime;
    private boolean directionFlag;
    private String antiKeyPrefix;
    private String antiKeySuffix;
    private String antiKeyInner;
    private String antiKeyRegex;
    private String antiMimeType;
    private String checkType;

    public FileFilterParams(IEntryParam entryParam) throws Exception {
        super(entryParam);
        try { keyPrefix = entryParam.getParamValue("f-prefix"); } catch (Exception e) {}
        try { keySuffix = entryParam.getParamValue("f-suffix"); } catch (Exception e) {}
        try { keyInner = entryParam.getParamValue("f-inner"); } catch (Exception e) {}
        try { keyRegex = entryParam.getParamValue("f-regex"); } catch (Exception e) {}
        try { pointDate = entryParam.getParamValue("f-date"); } catch (Exception e) { pointDate = ""; }
        try { pointTime = entryParam.getParamValue("f-time"); } catch (Exception e) { pointTime = ""; }
        try { direction = entryParam.getParamValue("f-direction"); } catch (Exception e) { direction = ""; }
        try { mimeType = entryParam.getParamValue("f-mime"); } catch (Exception e) {}
        try { type = entryParam.getParamValue("f-type"); } catch (Exception e) { type = ""; }
        try { status = entryParam.getParamValue("f-status"); } catch (Exception e) { status = ""; }
        datetime = getPointDatetime();
        if (!"".equals(pointDate)) directionFlag = getDirection();
        try { antiKeyPrefix = entryParam.getParamValue("f-anti-prefix"); } catch (Exception e) {}
        try { antiKeySuffix = entryParam.getParamValue("f-anti-suffix"); } catch (Exception e) {}
        try { antiKeyInner = entryParam.getParamValue("f-anti-inner"); } catch (Exception e) {}
        try { antiKeyRegex = entryParam.getParamValue("f-anti-regex"); } catch (Exception e) {}
        try { antiMimeType = entryParam.getParamValue("f-anti-mime"); } catch (Exception e) {}
        try { checkType = entryParam.getParamValue("f-check"); } catch (Exception e) {}
    }

    public List<String> getKeyPrefix() {
        if (keyPrefix != null && !"".equals(keyPrefix)) return Arrays.asList(keyPrefix.split(","));
        else return null;
    }

    public List<String> getKeySuffix() {
        if (keySuffix != null && !"".equals(keySuffix)) return Arrays.asList(keySuffix.split(","));
        else return null;
    }

    public List<String>  getKeyInner() {
        if (keyInner != null && !"".equals(keyInner)) return Arrays.asList(keyInner.split(","));
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

    public List<String> getMimeType() {
        if (mimeType != null && !"".equals(mimeType)) return Arrays.asList(mimeType.split(","));
        else return null;
    }

    public int getType() throws IOException {
        if ("".equals(type)) {
            return -1;
        } else if (type.matches("([01])")) {
            return Integer.valueOf(type);
        } else {
            throw new IOException("no incorrect type, please set it 0/1.");
        }
    }

    public int getStatus() throws IOException {
        if ("".equals(status)) {
            return -1;
        } else if (status.matches("([01])")) {
            return Integer.valueOf(status);
        } else {
            throw new IOException("no incorrect status, please set it 0/1.");
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

    public List<String>  getAntiKeyInner() {
        if (antiKeyInner != null && !"".equals(antiKeyInner)) return Arrays.asList(antiKeyInner.split(","));
        else return null;
    }

    public List<String> getAntiKeyRegex() {
        if (antiKeyRegex != null && !"".equals(antiKeyRegex)) return Arrays.asList(antiKeyRegex.split(","));
        else return null;
    }

    public List<String> getAntiMimeType() {
        if (antiMimeType != null && !"".equals(antiMimeType)) return Arrays.asList(antiMimeType.split(","));
        else return null;
    }

    public String getCheckType() {
        return checkType;
    }
}
