package com.qiniu.interfaces;

import com.qiniu.common.QiniuException;

public interface IOssFileProcess {

    IOssFileProcess clone() throws CloneNotSupportedException;

    QiniuException qiniuException();

    void processFile(String fileInfoStr, int retryCount);

    void batchProcessFile(String fileInfoStr, int retryCount);

    void closeResource();
}