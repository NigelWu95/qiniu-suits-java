package com.qiniu.interfaces;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.model.FileInfo;

public interface IOssFileProcess {

    IOssFileProcess clone() throws CloneNotSupportedException;

    QiniuException qiniuException();

    void processFile(FileInfo fileInfo, int retryCount, boolean batch);

    void checkBatchProcess(int retryCount);

    void closeResource();
}