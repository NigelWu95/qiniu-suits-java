package com.qiniu.interfaces;

import com.qiniu.common.QiniuException;

import java.io.IOException;

public interface IBucketProcess {

    void processBucketWithEndFile(IOssFileProcess iOssFileProcessor, boolean processBatch, int version, int maxThreads,
            boolean withParallel, int level, int unitLen) throws IOException, CloneNotSupportedException;

    void processBucketWithPrefix(IOssFileProcess iOssFileProcessor, boolean processBatch, int version, int maxThreads,
            boolean withParallel, int level, int unitLen) throws IOException, CloneNotSupportedException;

    void closeResource();
}