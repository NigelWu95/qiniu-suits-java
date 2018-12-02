package com.qiniu.service.interfaces;

import com.qiniu.common.QiniuException;

import java.util.List;

public interface ILineProcess<T> {

    ILineProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException;

    default void setBatch(boolean batch) {}

    void setRetryCount(int retryCount);

    default String getProcessName() {
        return "";
    }

    default String getInfo() {
        return "";
    }

    void processLine(List<T> list) throws QiniuException;

    void closeResource();
}
