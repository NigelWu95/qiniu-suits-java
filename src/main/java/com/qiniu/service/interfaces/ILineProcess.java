package com.qiniu.service.interfaces;

import com.qiniu.common.QiniuException;

import java.util.List;

public interface ILineProcess<T> {

    ILineProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException;

    void setBatch(boolean batch);

    void setRetryCount(int retryCount);

    String getProcessName();

    String getInfo();

    void processLine(List<T> list) throws QiniuException;

    void closeResource();
}
