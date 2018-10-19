package com.qiniu.common;

import com.qiniu.storage.model.FileInfo;

import java.util.List;

public class ListFileAntiFilter {

    private List<String> keyPrefix;
    private List<String> keySuffix;
    private List<String> keyRegex;
    private List<String> mime;

    public void setKeyPrefix(List<String> keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    public void setKeySuffix(List<String> keySuffix) {
        this.keySuffix = keySuffix;
    }

    public void setKeyRegex(List<String> keyRegex) {
        this.keyRegex = keyRegex;
    }

    public void setMime(List<String> mime) {
        this.mime = mime;
    }

    private boolean filterKeyPrefixAndSuffix(FileInfo fileInfo) {

        if ((keyPrefix == null || keyPrefix.size() == 0) && (keySuffix == null || keySuffix.size() == 0)) return true;
        else if (keySuffix == null || keySuffix.size() == 0) return keyPrefix.stream().anyMatch(prefix -> fileInfo.key.startsWith(prefix));
        else if (keyPrefix == null || keyPrefix.size() == 0) return keySuffix.stream().anyMatch(suffix -> fileInfo.key.endsWith(suffix));
        else return keyPrefix.stream().anyMatch(prefix -> fileInfo.key.startsWith(prefix)) &&
                    keySuffix.stream().anyMatch(suffix -> fileInfo.key.endsWith(suffix));
    }

    private boolean filterKeyRegex(FileInfo fileInfo) {

        if (keyRegex == null || keyRegex.size() == 0) return true;
        else return keyRegex.stream().anyMatch(regex -> fileInfo.key.matches(regex));
    }

    private boolean filterMime(FileInfo fileInfo) {

        if (mime == null || mime.size() == 0) return true;
        else return mime.stream().anyMatch(mime -> fileInfo.mimeType.contains(mime));
    }

    public boolean doFileAntiFilter(FileInfo fileInfo) {
        if (fileInfo == null) return false;
        boolean keyFilter = filterKeyPrefixAndSuffix(fileInfo) && filterKeyRegex(fileInfo);
        boolean mimeFilter = filterMime(fileInfo);
        return !(keyFilter && mimeFilter);
    }
}