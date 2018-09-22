package com.qiniu.interfaces;

public interface IOssFileProcess {

    void processFile(String fileInfoStr);

    void closeResource();
}