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

    private boolean filterMime(FileInfo fileInfo) {
        if (checkList(mime)) return mime.stream().anyMatch(mime -> fileInfo.mimeType.contains(mime));
        else return true;
    }

    public boolean doFileAntiFilter(FileInfo fileInfo) {
        if (fileInfo == null) return false;
        boolean keyFilter = filterKeyPrefixAndSuffix(fileInfo) && filterKeyRegex(fileInfo);
        boolean mimeFilter = filterMime(fileInfo);
        return !(keyFilter && mimeFilter);
    }

    private boolean checkList(List<String> list) {
        return list != null && list.size() != 0;
    }

    public boolean isValid() {
        return checkList(keyPrefix) || checkList(keySuffix) || checkList(keyRegex) || checkList(mime);
    }
}
