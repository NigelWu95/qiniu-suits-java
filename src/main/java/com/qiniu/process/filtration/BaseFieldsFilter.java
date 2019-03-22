package com.qiniu.process.filtration;

import java.util.List;
import java.util.Map;

public class BaseFieldsFilter {

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

    public void setKeyConditions(List<String> keyPrefix, List<String> keySuffix, List<String> keyInner,
                                 List<String> keyRegex) {
        this.keyPrefix = keyPrefix;
        this.keySuffix = keySuffix;
        this.keyInner = keyInner;
        this.keyRegex = keyRegex;
    }

    public void setAntiKeyConditions(List<String> antiKeyPrefix, List<String> antiKeySuffix, List<String> antiKeyInner,
                                     List<String> antiKeyRegex) {
        this.antiKeyPrefix = antiKeyPrefix;
        this.antiKeySuffix = antiKeySuffix;
        this.antiKeyInner = antiKeyInner;
        this.antiKeyRegex = antiKeyRegex;
    }

    public void setMimeTypeConditions(List<String> mimeType, List<String> antiMimeType) {
        this.mimeType = mimeType;
        this.antiMimeType = antiMimeType;
    }

    public void setOtherConditions(long putTimeMin, long putTimeMax, String type, String status) {
        this.putTimeMin = putTimeMin;
        this.putTimeMax = putTimeMax;
        this.type = type == null ? "" : type;
        this.status = status == null ? "" : status;
    }

    private boolean checkList(List<String> list) {
        return list != null && list.size() != 0;
    }

    public boolean isValid() {
        return (checkList(keyPrefix) || checkList(keySuffix) || checkList(keyInner) || checkList(keyRegex) ||
                checkList(mimeType) || (putTimeMax > putTimeMin && putTimeMin >= 0) || type.matches("[01]") ||
                status.matches("[01]") || checkList(antiKeyPrefix) || checkList(antiKeySuffix) ||
                checkList(antiKeyInner) || checkList(antiKeyRegex) || checkList(antiMimeType));
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

    public boolean checkMime() {
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

    public boolean checkAntiMime() {
        return checkList(antiMimeType);
    }

    public boolean filterKeyPrefix(Map<String, String> item) {
        if (checkItem(item, "key")) return false;
        else return keyPrefix.stream().anyMatch(prefix -> item.get("key").startsWith(prefix));
    }

    public boolean filterKeySuffix(Map<String, String> item) {
        if (checkItem(item, "key")) return false;
        else return keySuffix.stream().anyMatch(suffix -> item.get("key").endsWith(suffix));
    }

    public boolean filterKeyInner(Map<String, String> item) {
        if (checkItem(item, "key")) return false;
        else return keyInner.stream().anyMatch(inner -> item.get("key").contains(inner));
    }

    public boolean filterKeyRegex(Map<String, String> item) {
        if (checkItem(item, "key")) return false;
        else return keyRegex.stream().anyMatch(regex -> item.get("key").matches(regex));
    }

    public boolean filterPutTime(Map<String, String> item) {
        if (checkItem(item, "putTime")) return false;
        else return Long.valueOf(item.get("putTime")) <= putTimeMax && putTimeMin <= Long.valueOf(item.get("putTime"));
    }

    public boolean filterMimeType(Map<String, String> item) {
        if (checkItem(item, "mimeType")) return false;
        else return mimeType.stream().anyMatch(mimeType -> item.get("mimeType").contains(mimeType));
    }

    public boolean filterType(Map<String, String> item) {
        if (checkItem(item, "type")) return false;
        else return item.get("type").equals(type);
    }

    public boolean filterStatus(Map<String, String> item) {
        if (checkItem(item, "status")) return false;
        else return item.get("status").equals(status);
    }

    public boolean filterAntiKeyPrefix(Map<String, String> item) {
        if (checkItem(item, "key")) return true;
        else return antiKeyPrefix.stream().noneMatch(prefix -> item.get("key").startsWith(prefix));
    }

    public boolean filterAntiKeySuffix(Map<String, String> item) {
        if (checkItem(item, "key")) return true;
        else return antiKeySuffix.stream().noneMatch(suffix -> item.get("key").endsWith(suffix));
    }

    public boolean filterAntiKeyInner(Map<String, String> item) {
        if (checkItem(item, "key")) return true;
        else return antiKeyInner.stream().noneMatch(inner -> item.get("key").contains(inner));
    }

    public boolean filterAntiKeyRegex(Map<String, String> item) {
        if (checkItem(item, "key")) return true;
        else return antiKeyRegex.stream().noneMatch(regex -> item.get("key").matches(regex));
    }

    public boolean filterAntiMimeType(Map<String, String> item) {
        if (checkItem(item, "mimeType")) return true;
        else return antiMimeType.stream().noneMatch(mimeType -> item.get("mimeType").contains(mimeType));
    }

    private boolean checkItem(Map<String, String> item, String key) {
        return item == null || item.get(key) == null || "".equals(item.get(key));
    }
}
