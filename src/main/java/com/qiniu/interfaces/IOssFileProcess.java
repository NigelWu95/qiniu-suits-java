package com.qiniu.interfaces;

import com.qiniu.storage.model.FileInfo;

public interface IOssFileProcess {

    void processFile(FileInfo fileInfo);

    void closeResource();
}