package com.qiniu.service.process;
import com.qiniu.service.interfaces.ILineFilter;

import java.util.List;
import java.util.Map;

public class FileFilter {

    private List<String> keyPrefix;
    private List<String> keySuffix;
    private List<String> keyRegex;
    private long putTimeMin;
    private long putTimeMax;
    private List<String> mime;
    private int type;
    private List<String> antiKeyPrefix;
    private List<String> antiKeySuffix;
    private List<String> antiKeyRegex;
    private List<String> antiMime;

    public void setKeyConditions(List<String> keyPrefix, List<String> keySuffix, List<String> keyRegex) {
        this.keyPrefix = keyPrefix;
        this.keySuffix = keySuffix;
        this.keyRegex = keyRegex;
    }

    public void setAntiKeyConditions(List<String> antiKeyPrefix, List<String> antiKeySuffix, List<String> antiKeyRegex) {
        this.antiKeyPrefix = antiKeyPrefix;
        this.antiKeySuffix = antiKeySuffix;
        this.antiKeyRegex = antiKeyRegex;
    }

    public void setMimeConditions(List<String> mime, List<String> antiMime) {
        this.mime = mime;
        this.antiMime = antiMime;
    }

    public void setOtherConditions(long putTimeMax, long putTimeMin, int type) {
        this.putTimeMax = putTimeMax;
        this.putTimeMin = putTimeMin;
        this.type = type;
    }

    public boolean checkKeyPrefix() {
        return checkList(keyPrefix);
    }

    public boolean checkKeySuffix() {
        return checkList(keySuffix);
    }

    public boolean checkKeyRegex() {
        return checkList(keyRegex);
    }

    public boolean checkPutTime() {
        return putTimeMin > 0 || putTimeMax > 0;
    }

    public boolean checkMime() {
        return checkList(mime);
    }

    public boolean checkType() {
        return type > -1;
    }

    public boolean checkAntiKeyPrefix() {
        return checkList(antiKeyPrefix);
    }

    public boolean checkAntiKeySuffix() {
        return checkList(antiKeySuffix);
    }

    public boolean checkAntiKeyRegex() {
        return checkList(antiKeyRegex);
    }

    public boolean checkAntiMime() {
        return checkList(antiMime);
    }

    public boolean filterKeyPrefix(Map<String, String> item) {
        return keyPrefix.stream().anyMatch(prefix -> item.get("key").startsWith(prefix));
    }

    public boolean filterKeySuffix(Map<String, String> item) {
        return keySuffix.stream().anyMatch(suffix -> item.get("key").endsWith(suffix));
    }

    public boolean filterKeyRegex(Map<String, String> item) {
        return keyRegex.stream().anyMatch(regex -> item.get("key").matches(regex));
    }

    public boolean filterPutTime(Map<String, String> item) {
        if (putTimeMax > 0) return Long.valueOf(item.get("putTime")) <= putTimeMax;
        else return putTimeMin <= Long.valueOf(item.get("putTime"));
    }

    public boolean filterMime(Map<String, String> item) {
        return mime.stream().anyMatch(mime -> item.get("mime").contains(mime));
    }

    public boolean filterType(Map<String, String> item) {
        return (Integer.valueOf(item.get("type")) == type);
    }

    public boolean filterAntiKeyPrefix(Map<String, String> item) {
        return antiKeyPrefix.stream().noneMatch(prefix -> item.get("key").startsWith(prefix));
    }

    public boolean filterAntiKeySuffix(Map<String, String> item) {
        return antiKeySuffix.stream().noneMatch(suffix -> item.get("key").endsWith(suffix));
    }

    public boolean filterAntiKeyRegex(Map<String, String> item) {
        return antiKeyRegex.stream().noneMatch(regex -> item.get("key").matches(regex));
    }

    public boolean filterAntiMime(Map<String, String> item) {
        return antiMime.stream().noneMatch(mime -> item.get("mime").contains(mime));
    }

    private boolean checkList(List<String> list) {
        return list != null && list.size() != 0;
    }

    public boolean isValid() {
        return (checkList(keyPrefix) || checkList(keySuffix) || checkList(keyRegex) || checkList(mime) ||
                putTimeMin > 0 || putTimeMax > 0 || type > -1 || checkList(antiKeyPrefix) || checkList(antiKeySuffix) ||
                checkList(antiKeyRegex) || checkList(antiMime));
    }
}
