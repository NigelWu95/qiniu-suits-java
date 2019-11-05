package com.qiniu.interfaces;

public interface KeyValuePair<K, Proto> {

    void put(K key, String value);

    void put(K key, Integer value);

    void put(K key, Long value);

    Proto getProtoEntity();

    int size();
}
