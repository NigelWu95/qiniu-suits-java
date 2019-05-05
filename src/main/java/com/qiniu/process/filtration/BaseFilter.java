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
        if (!checkKey() && !checkMimeType() && !checkPutTime() && !checkType() && !checkStatus())
            throw new IOException("all conditions are invalid.");
    }

    private boolean checkList(List<String> list) {
        return list != null && list.size() != 0;
    }

    public boolean checkKey() {
        return checkList(keyPrefix) || checkList(keySuffix) || checkList(keyInner) || checkList(keyRegex) ||
                checkList(antiKeyPrefix) || checkList(antiKeySuffix) || checkList(antiKeyInner) || checkList(antiKeyRegex);
    }

    public boolean checkMimeType() {
        return checkList(mimeType) || checkList(antiMimeType);
    }

    public boolean checkPutTime() {
        return putTimeMax > putTimeMin && putTimeMin >= 0;
    }

    public boolean checkType() {
        return "0".equals(type) || "1".equals(type);
    }

    public boolean checkStatus() {
        return "0".equals(status) || "1".equals(status);
    }

    public boolean filterKey(T item) {
        if (checkItem(item, "key")) {
            return false;
        } else {
            return (keyPrefix == null || keyPrefix.stream().anyMatch(prefix -> valueFrom(item, "key").startsWith(prefix)))
                    && (keySuffix == null || keySuffix.stream().anyMatch(suffix -> valueFrom(item, "key").endsWith(suffix)))
                    && (keyInner == null || keyInner.stream().anyMatch(inner -> valueFrom(item, "key").contains(inner)))
                    && (keyRegex == null || keyRegex.stream().anyMatch(regex -> valueFrom(item, "key").matches(regex)))
                    && (antiKeyPrefix == null || antiKeyPrefix.stream().noneMatch(prefix -> valueFrom(item, "key").startsWith(prefix)))
                    && (antiKeySuffix == null || antiKeySuffix.stream().noneMatch(suffix -> valueFrom(item, "key").endsWith(suffix)))
                    && (antiKeyInner == null || antiKeyInner.stream().noneMatch(inner -> valueFrom(item, "key").contains(inner)))
                    && (antiKeyRegex == null || antiKeyRegex.stream().noneMatch(regex -> valueFrom(item, "key").matches(regex)));
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
