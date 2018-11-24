package com.qiniu.service.interfaces;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.model.FileInfo;

import java.util.List;

public interface IQossProcess {

    IQossProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException;

    void setBatch(boolean batch);

    void setRetryCount(int retryCount);

    String getProcessName();

    String getInfo();

    void processFile(List<FileInfo> fileInfoList) throws QiniuException;

    void closeResource();
}
