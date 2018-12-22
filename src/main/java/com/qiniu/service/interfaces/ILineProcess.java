package com.qiniu.service.interfaces;

import com.qiniu.common.QiniuException;

import java.util.List;
import java.util.Map;

public interface ILineProcess<T> {

    ILineProcess<T> getNewInstance(int resultFileIndex) throws CloneNotSupportedException;

    default String getProcessName() {
        return "";
    }

    default void setRetryCount(int retryCount) {}

    default void setBatch(boolean batch) {}

    void processLine(List<T> list) throws QiniuException;

    default void setNextProcessor(ILineProcess<Map<String, String>> nextProcessor) {}

    void closeResource();
}
