package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;
import com.qiniu.util.DateUtils;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

public class FileFilterParams extends FileInputParams {

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
    private boolean directionFlag;
    private String antiKeyPrefix;
    private String antiKeySuffix;
    private String antiKeyInner;
    private String antiKeyRegex;
    private String antiMimeType;
    private String checkType;

    public FileFilterParams(IEntryParam entryParam) throws Exception {
        super(entryParam);
        try { keyPrefix = entryParam.getParamValue("f-prefix"); } catch (Exception e) { keyPrefix = ""; }
        try { keySuffix = entryParam.getParamValue("f-suffix"); } catch (Exception e) { keySuffix = ""; }
        try { keyInner = entryParam.getParamValue("f-inner"); } catch (Exception e) { keyPrefix = ""; }
        try { keyRegex = entryParam.getParamValue("f-regex"); } catch (Exception e) { keyInner = ""; }
        try { pointDate = entryParam.getParamValue("f-date"); } catch (Exception e) { pointDate = ""; }
        try { pointTime = entryParam.getParamValue("f-time"); } catch (Exception e) { pointTime = ""; }
        try { direction = entryParam.getParamValue("f-direction"); } catch (Exception e) { direction = ""; }
        try { mimeType = entryParam.getParamValue("f-mime"); } catch (Exception e) { mimeType = ""; }
        try { type = entryParam.getParamValue("f-type"); } catch (Exception e) { type = ""; }
        try { status = entryParam.getParamValue("f-status"); } catch (Exception e) { status = ""; }
        if (!"".equals(pointDate)) directionFlag = getDirection();
        try { antiKeyPrefix = entryParam.getParamValue("f-anti-prefix"); } catch (Exception e) { antiKeyPrefix = ""; }
        try { antiKeySuffix = entryParam.getParamValue("f-anti-suffix"); } catch (Exception e) { antiKeySuffix = ""; }
        try { antiKeyInner = entryParam.getParamValue("f-anti-inner"); } catch (Exception e) { antiKeyInner = ""; }
        try { antiKeyRegex = entryParam.getParamValue("f-anti-regex"); } catch (Exception e) { antiKeyRegex = ""; }
        try { antiMimeType = entryParam.getParamValue("f-anti-mime"); } catch (Exception e) { antiMimeType = ""; }
        try { checkType = entryParam.getParamValue("f-check"); } catch (Exception e) { checkType = ""; }
    }

    private List<String> getFilterValues(String key, String field, String name) throws IOException {
        if (!"".equals(field)) {
            if (!getIndexMap().containsValue(key)) {
                throw new IOException("f-" + name + " filter must get the " + key + "'s index.");
            }
            return Arrays.asList(field.split(","));
        }
        else return null;
    }

    public List<String> getKeyPrefix() throws IOException {
        return getFilterValues("key", keyPrefix, "prefix");
    }

    public List<String> getKeySuffix() throws IOException {
        return getFilterValues("key", keySuffix, "suffix");
    }

    public List<String> getKeyInner() throws IOException {
        return getFilterValues("key", keyInner, "inner");
    }

    public List<String> getKeyRegex() throws IOException {
        return getFilterValues("key", keyRegex, "regix");
    }

    private Long getPointDatetime() throws IOException, ParseException {
        String pointDatetime;
        if(pointDate.matches("\\d{4}-\\d{2}-\\d{2}")) {
            if (!getIndexMap().containsValue("putTime")) {
                throw new IOException("f-date filter must get the putTime's index.");
            }
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

    public long getPutTimeMax() throws IOException, ParseException {
        if (directionFlag) return getPointDatetime() * 10000;
        return 0;
    }

    public long getPutTimeMin() throws IOException, ParseException {
        if (!directionFlag) return getPointDatetime() * 10000;
        return 0;
    }

    public List<String> getMimeType() throws IOException {
        return getFilterValues("mimeType", mimeType, "mime");
    }

    public int getType() throws IOException {
        if ("".equals(type)) {
            return -1;
        } else if (type.matches("([01])")) {
            if (!getIndexMap().containsValue("type")) {
                throw new IOException("f-type filter must get the type's index.");
            }
            return Integer.valueOf(type);
        } else {
            throw new IOException("no incorrect type, please set it 0/1.");
        }
    }

    public int getStatus() throws IOException {
        if ("".equals(status)) {
            return -1;
        } else if (status.matches("([01])")) {
            if (!getIndexMap().containsValue("status")) {
                throw new IOException("f-status filter must get the status's index.");
            }
            return Integer.valueOf(status);
        } else {
            throw new IOException("no incorrect status, please set it 0/1.");
        }
    }

    public List<String> getAntiKeyPrefix() throws IOException {
        return getFilterValues("key", antiKeyPrefix, "anti-prefix");
    }

    public List<String> getAntiKeySuffix() throws IOException {
        return getFilterValues("key", antiKeySuffix, "anti-suffix");
    }

    public List<String>  getAntiKeyInner() throws IOException {
        return getFilterValues("key", antiKeyInner, "anti-inner");
    }

    public List<String> getAntiKeyRegex() throws IOException {
        return getFilterValues("key", antiKeyRegex, "anti-regix");
    }

    public List<String> getAntiMimeType() throws IOException {
        return getFilterValues("mime", antiMimeType, "anti-mime");
    }

    public String getCheckType() {
        return checkType;
    }
}
