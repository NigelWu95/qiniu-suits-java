package com.qiniu.service.interfaces;

import com.qiniu.common.ListFileAntiFilter;
import com.qiniu.common.ListFileFilter;
import com.qiniu.common.QiniuException;
import com.qiniu.storage.model.FileInfo;

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

    default void setFilter(ListFileFilter listFileFilter, ListFileAntiFilter listFileAntiFilter) {}

    void processLine(List<T> list) throws QiniuException;

    default void setNextProcessor(ILineProcess<FileInfo> nextProcessor) {}

    void closeResource();
}
