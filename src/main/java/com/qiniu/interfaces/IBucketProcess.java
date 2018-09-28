package com.qiniu.interfaces;

import com.qiniu.common.QiniuException;

public interface IBucketProcess {

    void processBucket(IOssFileProcess iOssFileProcessor, int version, int maxThreads, boolean withParallel,
                       boolean secondLevel, int unitLen) throws QiniuException;

    void closeResource();
}