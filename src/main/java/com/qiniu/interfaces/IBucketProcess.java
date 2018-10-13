package com.qiniu.interfaces;

import com.qiniu.common.QiniuException;

import java.io.IOException;

public interface IBucketProcess {

    void processBucket(IOssFileProcess iOssFileProcessor, boolean processBatch, int version, int maxThreads, int level,
                       int unitLen, boolean endFile) throws IOException, CloneNotSupportedException;
}