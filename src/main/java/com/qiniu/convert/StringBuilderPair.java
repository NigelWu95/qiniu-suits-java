package com.qiniu.convert;

import com.qiniu.interfaces.KeyValuePair;
import com.qiniu.util.URLUtils;

public class StringBuilderPair implements KeyValuePair<String, String> {

    private StringBuilder stringBuilder = new StringBuilder();
    private String separator;
    private int size;

    public StringBuilderPair(String separator) {
        this.separator = separator;
    }

    @Override
    public void put(String key, String value) {
        stringBuilder.append(separator).append(URLUtils.getEncodedURI(value));
        size++;
    }

    @Override
    public void put(String key, Integer value) {
        stringBuilder.append(separator).append(value);
        size++;
    }

    @Override
    public void put(String key, Long value) {
        stringBuilder.append(separator).append(value);
        size++;
    }

    @Override
    public String getProtoEntity() {
        return stringBuilder.substring(1);
    }

    @Override
    public int size() {
        return size;
    }
}
