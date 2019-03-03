package com.qiniu.service.filtration;

import com.qiniu.util.LineUtils;

import java.util.ArrayList;
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
    private int type;
    private int status;
    private List<String> antiKeyPrefix;
    private List<String> antiKeySuffix;
    private List<String> antiKeyInner;
    private List<String> antiKeyRegex;
    private List<String> antiMimeType;

    public void setKeyConditions(List<String> keyPrefix, List<String> keySuffix, List<String> keyInner,
                                 List<String> keyRegex) {
        this.keyPrefix = keyPrefix == null ? new ArrayList<>() : keyPrefix;
        this.keySuffix = keySuffix == null ? new ArrayList<>() : keySuffix;
        this.keyInner = keyInner == null ? new ArrayList<>() : keyInner;
        this.keyRegex = keyRegex == null ? new ArrayList<>() : keyRegex;
    }

    public void setAntiKeyConditions(List<String> antiKeyPrefix, List<String> antiKeySuffix, List<String> antiKeyInner,
                                     List<String> antiKeyRegex) {
        this.antiKeyPrefix = antiKeyPrefix == null ? new ArrayList<>() : antiKeyPrefix;
        this.antiKeySuffix = antiKeySuffix == null ? new ArrayList<>() : antiKeySuffix;
        this.antiKeyInner = antiKeyInner == null ? new ArrayList<>() : antiKeyInner;
        this.antiKeyRegex = antiKeyRegex == null ? new ArrayList<>() : antiKeyRegex;
    }

    public void setMimeTypeConditions(List<String> mimeType, List<String> antiMimeType) {
        this.mimeType = mimeType == null ? new ArrayList<>() : mimeType;
        this.antiMimeType = antiMimeType == null ? new ArrayList<>() : antiMimeType;
    }

    public void setOtherConditions(long putTimeMax, long putTimeMin, int type, int status) {
        this.putTimeMax = putTimeMax;
        this.putTimeMin = putTimeMin;
        this.type = type;
        this.status = status;
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
        return putTimeMin > 0 || putTimeMax > 0;
    }

    public boolean checkMime() {
        return checkList(mimeType);
    }

    public boolean checkType() {
        return type == 0 || type == 1;
    }

    public boolean checkStatus() {
        return status == 0 || status == 1;
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
        if (LineUtils.checkItem(item, "key")) return false;
        else return keyPrefix.stream().anyMatch(prefix -> item.get("key").startsWith(prefix));
    }

    public boolean filterKeySuffix(Map<String, String> item) {
        if (LineUtils.checkItem(item, "key")) return false;
        else return keySuffix.stream().anyMatch(suffix -> item.get("key").endsWith(suffix));
    }

    public boolean filterKeyInner(Map<String, String> item) {
        if (LineUtils.checkItem(item, "key")) return false;
        else return keyInner.stream().anyMatch(inner -> item.get("key").contains(inner));
    }

    public boolean filterKeyRegex(Map<String, String> item) {
        if (LineUtils.checkItem(item, "key")) return false;
        else return keyRegex.stream().anyMatch(regex -> item.get("key").matches(regex));
    }

    public boolean filterPutTime(Map<String, String> item) {
        if (LineUtils.checkItem(item, "putTime")) return false;
        else if (putTimeMax > 0) return Long.valueOf(item.get("putTime")) <= putTimeMax;
        else return putTimeMin <= Long.valueOf(item.get("putTime"));
    }

    public boolean filterMimeType(Map<String, String> item) {
        if (LineUtils.checkItem(item, "mimeType")) return false;
        else return mimeType.stream().anyMatch(mimeType -> item.get("mimeType").contains(mimeType));
    }

    public boolean filterType(Map<String, String> item) {
        if (LineUtils.checkItem(item, "type")) return false;
        else return (Integer.valueOf(item.get("type")) == type);
    }

    public boolean filterStatus(Map<String, String> item) {
        if (LineUtils.checkItem(item, "status")) return false;
        else return (Integer.valueOf(item.get("status")) == status);
    }

    public boolean filterAntiKeyPrefix(Map<String, String> item) {
        if (LineUtils.checkItem(item, "key")) return true;
        else return antiKeyPrefix.stream().noneMatch(prefix -> item.get("key").startsWith(prefix));
    }

    public boolean filterAntiKeySuffix(Map<String, String> item) {
        if (LineUtils.checkItem(item, "key")) return true;
        else return antiKeySuffix.stream().noneMatch(suffix -> item.get("key").endsWith(suffix));
    }

    public boolean filterAntiKeyInner(Map<String, String> item) {
        if (LineUtils.checkItem(item, "key")) return true;
        else return antiKeyInner.stream().noneMatch(inner -> item.get("key").contains(inner));
    }

    public boolean filterAntiKeyRegex(Map<String, String> item) {
        if (LineUtils.checkItem(item, "key")) return true;
        else return antiKeyRegex.stream().noneMatch(regex -> item.get("key").matches(regex));
    }

    public boolean filterAntiMimeType(Map<String, String> item) {
        if (LineUtils.checkItem(item, "mimeType")) return true;
        else return antiMimeType.stream().noneMatch(mimeType -> item.get("mimeType").contains(mimeType));
    }

    private boolean checkList(List<String> list) {
        return list != null && list.size() != 0;
    }

    public boolean isValid() {
        return (checkList(keyPrefix) || checkList(keySuffix) || checkList(keyInner) || checkList(keyRegex) ||
                checkList(mimeType) || putTimeMin > 0 || putTimeMax > 0 || (type == 0 || type == 1) ||
                (status == 0 || status == 1) || checkList(antiKeyPrefix) || checkList(antiKeySuffix) ||
                checkList(antiKeyInner) || checkList(antiKeyRegex) || checkList(antiMimeType));
    }
}
