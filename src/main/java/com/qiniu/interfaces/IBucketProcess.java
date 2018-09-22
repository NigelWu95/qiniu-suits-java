package com.qiniu.interfaces;

public interface IBucketProcess {

    void processBucket();

    void processBucketV2(boolean withParallel);

    void closeResource();
}