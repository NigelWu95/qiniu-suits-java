package com.qiniu.interfaces;

public interface IBucketProcess {

    void processBucket();

    void processBucketV2();

    void closeResource();
}