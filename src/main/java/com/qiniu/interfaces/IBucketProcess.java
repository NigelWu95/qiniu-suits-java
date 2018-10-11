package com.qiniu.interfaces;

import com.qiniu.common.QiniuException;

public interface IBucketProcess {

    void processBucketWithEndFile(IOssFileProcess iOssFileProcessor, int version, int maxThreads, boolean withParallel,
                                  int level, int unitLen) throws QiniuException, CloneNotSupportedException;

    void processBucketWithPrefix(IOssFileProcess iOssFileProcessor, int version, int maxThreads, boolean withParallel,
                                 int level, int unitLen) throws QiniuException, CloneNotSupportedException;

    void closeResource();
}