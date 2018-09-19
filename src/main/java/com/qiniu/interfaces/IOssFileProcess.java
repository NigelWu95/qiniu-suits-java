package com.qiniu.interfaces;

public interface IOssFileProcess {

    void processKey(String bucket, String key, short status);
}