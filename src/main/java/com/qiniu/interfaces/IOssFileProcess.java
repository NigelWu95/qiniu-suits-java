package com.qiniu.interfaces;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.model.FileInfo;

import java.util.List;

public interface IOssFileProcess {

    IOssFileProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException;

    String getProcessName();

    void processFile(List<FileInfo> fileInfoList, boolean batch, int retryCount) throws QiniuException;

    void closeResource();
}
