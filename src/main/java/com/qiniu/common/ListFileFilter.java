package com.qiniu.common;

import com.qiniu.storage.model.FileInfo;

import java.util.List;

public class ListFileFilter {

    private List<String> keyPrefix;
    private List<String> keySuffix;
    private List<String> keyRegex;
    private long putTimeMin;
    private long putTimeMax;
    private List<String> mime;
    private int type;

    public void setKeyPrefix(List<String> keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    public void setKeySuffix(List<String> keySuffix) {
        this.keySuffix = keySuffix;
    }

    public void setKeyRegex(List<String> keyRegex) {
        this.keyRegex = keyRegex;
    }

    public void setPutTimeMin(long putTimeMin) {
        this.putTimeMin = putTimeMin;
    }

    public void setPutTimeMax(long putTimeMax) {
        this.putTimeMax = putTimeMax;
    }

    public void setMime(List<String> mime) {
        this.mime = mime;
    }

    public void setType(int type) {
        this.type = type;
    }

    private boolean filterKeyPrefix(FileInfo fileInfo) {

        if (keyPrefix == null) return true;
        else return keyPrefix.stream().anyMatch(prefix -> fileInfo.key.startsWith(prefix));
    }

    private boolean filterKeySuffix(FileInfo fileInfo) {

        if (keySuffix == null) return true;
        else return keySuffix.stream().anyMatch(suffix -> fileInfo.key.endsWith(suffix));
    }

    private boolean filterKeyRegex(FileInfo fileInfo) {

        if (keyRegex == null) return true;
        else return keyRegex.stream().anyMatch(regex -> fileInfo.key.matches(regex));
    }

    private boolean filterPutTime(FileInfo fileInfo) {

        if (putTimeMin >= putTimeMax || putTimeMax == 0) return true;
        else return putTimeMin < fileInfo.putTime && fileInfo.putTime <= putTimeMax;
    }

    private boolean filterMime(FileInfo fileInfo) {

        if (mime == null) return true;
        else return mime.stream().anyMatch(mime -> fileInfo.mimeType.contains(mime));
    }

    private boolean filterType(FileInfo fileInfo) {

        if (type < 0) return false;
        else return (fileInfo.type == type);
    }

    public boolean doFileFilter(FileInfo fileInfo) {
        if (fileInfo == null) return false;
        boolean keyFilter = (filterKeyPrefix(fileInfo) || filterKeySuffix(fileInfo)) && filterKeyRegex(fileInfo);
        boolean putTimeFilter = filterPutTime(fileInfo);
        boolean mimeFilter = filterMime(fileInfo);
        boolean typeFilter = filterType(fileInfo);
        return keyFilter && putTimeFilter && mimeFilter && typeFilter;
    }
}