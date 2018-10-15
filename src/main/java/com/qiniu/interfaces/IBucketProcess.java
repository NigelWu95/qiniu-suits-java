package com.qiniu.interfaces;

import com.qiniu.common.QiniuException;

import java.io.IOException;

public interface IBucketProcess {

    void processBucket(int version, int maxThreads, int level, int unitLen, boolean endFile, String customPrefix,
                       IOssFileProcess iOssFileProcessor, boolean processBatch) throws IOException, CloneNotSupportedException;
}