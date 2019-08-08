package com.qiniu.process.filtration;

import com.qiniu.util.ConvertingUtils;

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
        try {
            if (item == null) return false;
            String key = valueFrom(item, "key");
            if (checkList(keyPrefix) && keyPrefix.stream().noneMatch(key::startsWith)) return false;
            if (checkList(keySuffix) && keySuffix.stream().noneMatch(key::endsWith)) return false;
            if (checkList(keyInner) && keyInner.stream().noneMatch(key::contains)) return false;
            if (checkList(keyRegex) && keyRegex.stream().noneMatch(key::matches)) return false;
            if (checkList(antiKeyPrefix) && antiKeyPrefix.stream().anyMatch(key::startsWith)) return false;
            if (checkList(antiKeySuffix) && antiKeySuffix.stream().anyMatch(key::endsWith)) return false;
            if (checkList(antiKeyInner) && antiKeyInner.stream().anyMatch(key::contains)) return false;
            if (checkList(antiKeyRegex) && antiKeyRegex.stream().anyMatch(key::matches)) return false;
            return true;
        } catch (Exception e) {
            return true;
        }
    }

    public boolean filterMimeType(T item) {
        try {
            if (item == null) return false;
            String mType = valueFrom(item, ConvertingUtils.defaultMimeField);
            if (checkList(mimeType) && mimeType.stream().noneMatch(mType::contains)) return false;
            if (checkList(antiMimeType) && antiMimeType.stream().anyMatch(mType::contains)) return false;
            return true;
        } catch (Exception e) {
            return true;
        }
    }

    public boolean filterDatetime(T item) {
        try {
            if (item == null) return false;
            LocalDateTime localDateTime = LocalDateTime.parse(valueFrom(item, ConvertingUtils.defaultDatetimeField));
            return localDateTime.compareTo(datetimeMax) <= 0 && localDateTime.compareTo(datetimeMin) >= 0;
        } catch (Exception e) {
            return true;
        }
    }

    public boolean filterType(T item) {
        try {
            if (item == null) return false;
            return valueFrom(item, ConvertingUtils.defaultTypeField).equals(type);
        } catch (NullPointerException e) {
            return true;
        }
    }

    public boolean filterStatus(T item) {
        try {
            if (item == null) return false;
            return valueFrom(item, ConvertingUtils.defaultStatusField).equals(status);
        } catch (NullPointerException e) {
            return true;
        }
    }

    protected abstract String valueFrom(T item, String key);
}
