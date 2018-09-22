package com.qiniu.interfaces;

public interface IBucketProcess {

    void processBucket(boolean secondLevel);

    void processBucketV2(boolean withParallel, boolean secondLevel);

    void closeResource();
}