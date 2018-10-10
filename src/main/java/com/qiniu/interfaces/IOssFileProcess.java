package com.qiniu.interfaces;

import com.qiniu.common.QiniuException;

public interface IOssFileProcess {

    QiniuException qiniuException();

    void processFile(String fileInfoStr, int retryCount);

    void closeResource();

//    IOssFileProcess clone() throws CloneNotSupportedException;
}