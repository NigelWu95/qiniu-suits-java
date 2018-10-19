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

    private boolean filterKeyPrefixAndSuffix(FileInfo fileInfo) {
        boolean keyPrefixCheck = checkList(keyPrefix);
        boolean keySuffixCheck = checkList(keySuffix);
        if (!keyPrefixCheck && !keySuffixCheck) return true;
        else if (!keySuffixCheck) return keyPrefix.stream().anyMatch(prefix -> fileInfo.key.startsWith(prefix));
        else if (!keyPrefixCheck) return keySuffix.stream().anyMatch(suffix -> fileInfo.key.endsWith(suffix));
        else return keyPrefix.stream().anyMatch(prefix -> fileInfo.key.startsWith(prefix)) &&
                    keySuffix.stream().anyMatch(suffix -> fileInfo.key.endsWith(suffix));
    }

    private boolean filterKeyRegex(FileInfo fileInfo) {
        if (checkList(keyRegex)) return keyRegex.stream().anyMatch(regex -> fileInfo.key.matches(regex));
        else return true;
    }

    private boolean filterPutTime(FileInfo fileInfo) {
        if (putTimeMin == 0 && putTimeMax == 0) return true;
        else if (putTimeMin == 0) return fileInfo.putTime <= putTimeMax;
        else return putTimeMin <= fileInfo.putTime;
    }

    private boolean filterMime(FileInfo fileInfo) {
        if (checkList(mime)) return mime.stream().anyMatch(mime -> fileInfo.mimeType.contains(mime));
        else return true;
    }

    private boolean filterType(FileInfo fileInfo) {
        if (type < 0) return true;
        else return (fileInfo.type == type);
    }

    public boolean doFileFilter(FileInfo fileInfo) {
        if (fileInfo == null) return false;
        boolean keyFilter = filterKeyPrefixAndSuffix(fileInfo) && filterKeyRegex(fileInfo);
        boolean putTimeFilter = filterPutTime(fileInfo);
        boolean mimeFilter = filterMime(fileInfo);
        boolean typeFilter = filterType(fileInfo);
        return keyFilter && putTimeFilter && mimeFilter && typeFilter;
    }

    private boolean checkList(List<String> list) {
        return list != null && list.size() != 0;
    }

    public boolean isValid() {
        return (checkList(keyPrefix) && checkList(keySuffix) && checkList(keyRegex) && checkList(mime) &&
                putTimeMin == 0 && putTimeMax == 0 && type < 0);
    }
}