package com.qiniu.interfaces;

public interface KeyValuePair<K, Proto> {

    String EMPTY = null;

    void put(K key, String value);

    void put(K key, Boolean value);

    void put(K key, Integer value);

    void put(K key, Long value);

    Proto getProtoEntity();

    int size();
}
