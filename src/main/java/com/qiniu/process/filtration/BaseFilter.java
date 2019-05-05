package com.qiniu.process.filtration;

import java.io.IOException;
import java.util.List;

public abstract class BaseFilter<T> {

    private List<String> keyPrefix;
    private List<String> keySuffix;
    private List<String> keyInner;
    private List<String> keyRegex;
    private long putTimeMin;
    private long putTimeMax;
    private List<String> mimeType;
    private String type;
    private String status;
    private List<String> antiKeyPrefix;
    private List<String> antiKeySuffix;
    private List<String> antiKeyInner;
    private List<String> antiKeyRegex;
    private List<String> antiMimeType;

    public BaseFilter(List<String> keyPrefix, List<String> keySuffix, List<String> keyInner, List<String> keyRegex,
                      List<String> antiKeyPrefix, List<String> antiKeySuffix, List<String> antiKeyInner,
                      List<String> antiKeyRegex, List<String> mimeType, List<String> antiMimeType, long putTimeMin,
                      long putTimeMax, String type, String status) throws IOException {
        this.keyPrefix = keyPrefix;
        this.keySuffix = keySuffix;
        this.keyInner = keyInner;
        this.keyRegex = keyRegex;
        this.antiKeyPrefix = antiKeyPrefix;
        this.antiKeySuffix = antiKeySuffix;
        this.antiKeyInner = antiKeyInner;
        this.antiKeyRegex = antiKeyRegex;
        this.mimeType = mimeType;
        this.antiMimeType = antiMimeType;
        this.putTimeMin = putTimeMin;
        this.putTimeMax = putTimeMax;
        this.type = type == null ? "" : type;
        this.status = status == null ? "" : status;
        if (!checkKeyCon() && !checkMimeTypeCon() && !checkPutTimeCon() && !checkTypeCon() && !checkStatusCon())
            throw new IOException("all conditions are invalid.");
    }

    private boolean checkList(List<String> list) {
        return list != null && list.size() != 0;
    }

    public boolean checkKeyCon() {
        return checkList(keyPrefix) || checkList(keySuffix) || checkList(keyInner) || checkList(keyRegex) ||
                checkList(antiKeyPrefix) || checkList(antiKeySuffix) || checkList(antiKeyInner) || checkList(antiKeyRegex);
    }

    public boolean checkMimeTypeCon() {
        return checkList(mimeType) || checkList(antiMimeType);
    }

    public boolean checkPutTimeCon() {
        return putTimeMax > putTimeMin && putTimeMin >= 0;
    }

    public boolean checkTypeCon() {
        return "0".equals(type) || "1".equals(type);
    }

    public boolean checkStatusCon() {
        return "0".equals(status) || "1".equals(status);
    }

    public boolean filterKey(T item) {
        if (checkItem(item, "key")) {
            return false;
        } else {
            boolean result = false;
            if (keyPrefix != null) {
                result = keyPrefix.stream().anyMatch(prefix -> valueFrom(item, "key").startsWith(prefix));
                if (!result) return false;
            }
            if (keySuffix != null) {
                result = keySuffix.stream().anyMatch(suffix -> valueFrom(item, "key").endsWith(suffix));
                if (!result) return false;
            }
            if (keyInner != null) {
                result = keyInner.stream().anyMatch(inner -> valueFrom(item, "key").contains(inner));
                if (!result) return false;
            }
            if (keyRegex != null) {
                result = keyRegex.stream().anyMatch(regex -> valueFrom(item, "key").matches(regex));
                if (!result) return false;
            }
            if (antiKeyPrefix != null) {
                result = antiKeyPrefix.stream().noneMatch(prefix -> valueFrom(item, "key").startsWith(prefix));
                if (!result) return false;
            }
            if (antiKeySuffix != null) {
                result = antiKeySuffix.stream().noneMatch(suffix -> valueFrom(item, "key").endsWith(suffix));
                if (!result) return false;
            }
            if (antiKeyInner != null) {
                result = antiKeyInner.stream().noneMatch(inner -> valueFrom(item, "key").contains(inner));
                if (!result) return false;
            }
            if (antiKeyRegex != null) result = antiKeyRegex.stream().noneMatch(regex -> valueFrom(item, "key").matches(regex));
            return result;
        }
    }

    public boolean filterMimeType(T item) {
        if (checkItem(item, "mimeType")) {
            return false;
        } else {
            return (mimeType == null || mimeType.stream().anyMatch(mimeType -> valueFrom(item, "mimeType").contains(mimeType)))
                    && (antiMimeType == null || antiMimeType.stream().noneMatch(mimeType -> valueFrom(item, "mimeType").contains(mimeType)));
        }
    }

    public boolean filterPutTime(T item) {
        if (checkItem(item, "putTime")) return false;
        else return Long.valueOf(valueFrom(item, "putTime")) <= putTimeMax && putTimeMin <= Long.valueOf(valueFrom(item, "putTime"));
    }

    public boolean filterType(T item) {
        if (checkItem(item, "type")) return false;
        else return valueFrom(item, "type").equals(type);
    }

    public boolean filterStatus(T item) {
        if (checkItem(item, "status")) return false;
        else return valueFrom(item, "status").equals(status);
    }

    protected abstract boolean checkItem(T item, String key);

    protected abstract String valueFrom(T item, String key);
}
