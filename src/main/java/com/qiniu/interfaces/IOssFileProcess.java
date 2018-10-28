package com.qiniu.interfaces;

import com.qiniu.common.QiniuException;

import java.util.List;

public interface IOssFileProcess {

    IOssFileProcess getNewInstance(String resultFileSuffix) throws CloneNotSupportedException;

    QiniuException qiniuException();

    void processFile(String fileKey, int retryCount);

    void processFile(List<String> keyList, int retryCount);

    void closeResource();
}