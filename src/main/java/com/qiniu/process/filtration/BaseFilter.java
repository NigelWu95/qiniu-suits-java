package com.qiniu.process.filtration;

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
                      List<String> mimeType, long putTimeMin, long putTimeMax, String type, String status) {
        this.keyPrefix = keyPrefix;
        this.keySuffix = keySuffix;
        this.keyInner = keyInner;
        this.keyRegex = keyRegex;
        this.mimeType = mimeType;
        this.putTimeMin = putTimeMin;
        this.putTimeMax = putTimeMax;
        this.type = type == null ? "" : type;
        this.status = status == null ? "" : status;
    }

    public void setAntiConditions(List<String> antiKeyPrefix, List<String> antiKeySuffix, List<String> antiKeyInner,
                                     List<String> antiKeyRegex, List<String> antiMimeType) {
        this.antiKeyPrefix = antiKeyPrefix;
        this.antiKeySuffix = antiKeySuffix;
        this.antiKeyInner = antiKeyInner;
        this.antiKeyRegex = antiKeyRegex;
        this.antiMimeType = antiMimeType;
    }

    private boolean checkList(List<String> list) {
        return list != null && list.size() != 0;
    }

    public boolean isValid() {
        return checkKey() || checkMimeType() || checkPutTime() || checkType() || checkStatus();
    }

    public boolean checkKey() {
        return checkList(keyPrefix) || checkList(keySuffix) || checkList(keyInner) || checkList(keyRegex) ||
                checkList(antiKeyPrefix) || checkList(antiKeySuffix) || checkList(antiKeyInner) || checkList(antiKeyRegex);
    }

    public boolean checkMime() {
        return checkList(mimeType) || checkList(antiMimeType);
    }

    public boolean checkKeyPrefix() {
        return checkList(keyPrefix);
    }

    public boolean checkKeySuffix() {
        return checkList(keySuffix);
    }

    public boolean checkKeyInner() {
        return checkList(keyInner);
    }

    public boolean checkKeyRegex() {
        return checkList(keyRegex);
    }

    public boolean checkPutTime() {
        return putTimeMax > putTimeMin && putTimeMin >= 0;
    }

    public boolean checkMimeType() {
        return checkList(mimeType);
    }

    public boolean checkType() {
        return "0".equals(type) || "1".equals(type);
    }

    public boolean checkStatus() {
        return "0".equals(status) || "1".equals(status);
    }

    public boolean checkAntiKeyPrefix() {
        return checkList(antiKeyPrefix);
    }

    public boolean checkAntiKeySuffix() {
        return checkList(antiKeySuffix);
    }

    public boolean checkAntiKeyInner() {
        return checkList(antiKeyInner);
    }

    public boolean checkAntiKeyRegex() {
        return checkList(antiKeyRegex);
    }

    public boolean checkAntiMimeType() {
        return checkList(antiMimeType);
    }

    public boolean filterKeyPrefix(T item) {
        if (checkItem(item, "key")) return false;
        else return keyPrefix.stream().anyMatch(prefix -> valueFrom(item, "key").startsWith(prefix));
    }

    public boolean filterKeySuffix(T item) {
        if (checkItem(item, "key")) return false;
        else return keySuffix.stream().anyMatch(suffix -> valueFrom(item, "key").endsWith(suffix));
    }

    public boolean filterKeyInner(T item) {
        if (checkItem(item, "key")) return false;
        else return keyInner.stream().anyMatch(inner -> valueFrom(item, "key").contains(inner));
    }

    public boolean filterKeyRegex(T item) {
        if (checkItem(item, "key")) return false;
        else return keyRegex.stream().anyMatch(regex -> valueFrom(item, "key").matches(regex));
    }

    public boolean filterPutTime(T item) {
        if (checkItem(item, "putTime")) return false;
        else return Long.valueOf(valueFrom(item, "putTime")) <= putTimeMax && putTimeMin <= Long.valueOf(valueFrom(item, "putTime"));
    }

    public boolean filterMimeType(T item) {
        if (checkItem(item, "mimeType")) return false;
        else return mimeType.stream().anyMatch(mimeType -> valueFrom(item, "mimeType").contains(mimeType));
    }

    public boolean filterType(T item) {
        if (checkItem(item, "type")) return false;
        else return valueFrom(item, "type").equals(type);
    }

    public boolean filterStatus(T item) {
        if (checkItem(item, "status")) return false;
        else return valueFrom(item, "status").equals(status);
    }

    public boolean filterAntiKeyPrefix(T item) {
        if (checkItem(item, "key")) return true;
        else return antiKeyPrefix.stream().noneMatch(prefix -> valueFrom(item, "key").startsWith(prefix));
    }

    public boolean filterAntiKeySuffix(T item) {
        if (checkItem(item, "key")) return true;
        else return antiKeySuffix.stream().noneMatch(suffix -> valueFrom(item, "key").endsWith(suffix));
    }

    public boolean filterAntiKeyInner(T item) {
        if (checkItem(item, "key")) return true;
        else return antiKeyInner.stream().noneMatch(inner -> valueFrom(item, "key").contains(inner));
    }

    public boolean filterAntiKeyRegex(T item) {
        if (checkItem(item, "key")) return true;
        else return antiKeyRegex.stream().noneMatch(regex -> valueFrom(item, "key").matches(regex));
    }

    public boolean filterAntiMimeType(T item) {
        if (checkItem(item, "mimeType")) return true;
        else return antiMimeType.stream().noneMatch(mimeType -> valueFrom(item, "mimeType").contains(mimeType));
    }

    protected abstract boolean checkItem(T item, String key);

    protected abstract String valueFrom(T item, String key);
}
