package com.qiniu.interfaces;

public interface IOssFileProcess {

    void processFile(String fileInfoStr);

    void processFile(String fileInfoStr, int retryCount);

    void closeResource();
}