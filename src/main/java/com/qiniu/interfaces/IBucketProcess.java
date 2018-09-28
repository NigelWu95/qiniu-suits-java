package com.qiniu.interfaces;

import com.qiniu.common.QiniuException;

public interface IBucketProcess {

    void processBucketWithEndFile(IOssFileProcess iOssFileProcessor, int version, int maxThreads, boolean withParallel,
                                  int level, int unitLen) throws QiniuException;

    void processBucketWithPrefix(IOssFileProcess iOssFileProcessor, int version, int maxThreads, boolean withParallel,
                                 int level, int unitLen) throws QiniuException;

    void closeResource();
}