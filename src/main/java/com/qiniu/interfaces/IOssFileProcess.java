package com.qiniu.interfaces;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.model.FileInfo;

import java.util.List;

public interface IOssFileProcess {

    IOssFileProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException;

    QiniuException qiniuException();

    String getProcessName();

    void processFile(String fileKey, int retryCount);

    void processFile(List<FileInfo> fileInfoList, int retryCount);

    void closeResource();
}
