package com.qiniu.service.interfaces;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.model.FileInfo;

import java.util.List;

public interface IOssFileProcess {

    IOssFileProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException;

    String getProcessName();

    void setBatch(boolean batch);

    void setRetryCount(int retryCount);

    void processFile(List<FileInfo> fileInfoList, int retryCount) throws QiniuException;

    void closeResource();
}
