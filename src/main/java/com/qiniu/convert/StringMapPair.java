package com.qiniu.convert;

import com.qiniu.interfaces.KeyValuePair;

import java.util.HashMap;
import java.util.Map;

public class StringMapPair implements KeyValuePair<String, Map<String, String>> {

    private Map<String, String> stringMap = new HashMap<>();

    @Override
    public void putKey(String key, String value) {
        stringMap.put(key, value);
    }

    @Override
    public void put(String key, String value) {
        stringMap.put(key, value);
    }

    @Override
    public void put(String key, Boolean value) {
        stringMap.put(key, String.valueOf(value));
    }

    @Override
    public void put(String key, Integer value) {
        stringMap.put(key, String.valueOf(value));
    }

    @Override
    public void put(String key, Long value) {
        stringMap.put(key, String.valueOf(value));
    }

    @Override
    public Map<String, String> getProtoEntity() {
        return stringMap;
    }

    @Override
    public int size() {
        return stringMap.size();
    }
}
