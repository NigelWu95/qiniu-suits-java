package com.qiniu.process.filtration;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

public abstract class BaseFilter<T> {

    private List<String> keyPrefix;
    private List<String> keySuffix;
    private List<String> keyInner;
    private List<String> keyRegex;
    private LocalDateTime datetimeMin;
    private LocalDateTime datetimeMax;
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
                      List<String> antiKeyRegex, List<String> mimeType, List<String> antiMimeType, LocalDateTime putTimeMin,
                      LocalDateTime putTimeMax, String type, String status) throws IOException {
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
        this.datetimeMin = putTimeMin;
        this.datetimeMax = putTimeMax;
        this.type = type == null ? "" : type;
        this.status = status == null ? "" : status;
        if (!checkKeyCon() && !checkMimeTypeCon() && !checkDatetimeCon() && !checkTypeCon() && !checkStatusCon())
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

    public boolean checkDatetimeCon() {
        return datetimeMin != null && datetimeMax != null && datetimeMax.compareTo(datetimeMin) > 0;
    }

    public boolean checkTypeCon() {
        return type != null && !"".equals(type);
    }

    public boolean checkStatusCon() {
        return status != null && !"".equals(status);
    }

    public boolean filterKey(T item) {
        if (checkItem(item, "key")) {
            return false;
        } else {
            String key = valueFrom(item, "key");
            boolean result = false;
            if (checkList(keyPrefix)) {
                result = keyPrefix.stream().anyMatch(key::startsWith);
                if (!result) return false;
            }
            if (checkList(keySuffix)) {
                result = keySuffix.stream().anyMatch(key::endsWith);
                if (!result) return false;
            }
            if (checkList(keyInner)) {
                result = keyInner.stream().anyMatch(key::contains);
                if (!result) return false;
            }
            if (checkList(keyRegex)) {
                result = keyRegex.stream().anyMatch(key::matches);
                if (!result) return false;
            }
            if (checkList(antiKeyPrefix)) {
                result = antiKeyPrefix.stream().noneMatch(key::startsWith);
                if (!result) return false;
            }
            if (checkList(antiKeySuffix)) {
                result = antiKeySuffix.stream().noneMatch(key::endsWith);
                if (!result) return false;
            }
            if (checkList(antiKeyInner)) {
                result = antiKeyInner.stream().noneMatch(key::contains);
                if (!result) return false;
            }
            if (checkList(antiKeyRegex)) result = antiKeyRegex.stream().noneMatch(key::matches);
            return result;
        }
    }

    public boolean filterMimeType(T item) {
        if (checkItem(item, "mime")) {
            return false;
        } else {
            String mType = valueFrom(item, "mime");
            return (checkList(mimeType) || mimeType.stream().anyMatch(mType::contains))
                    && (checkList(antiMimeType) || antiMimeType.stream().noneMatch(mType::contains));
        }
    }

    public boolean filterDatetime(T item) {
        if (checkItem(item, "datetime")) return false;
        else {
            LocalDateTime localDateTime = LocalDateTime.parse(valueFrom(item, "datetime"));
            return localDateTime.compareTo(datetimeMax) <= 0 && localDateTime.compareTo(datetimeMin) >= 0;
        }
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
