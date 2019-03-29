package com.qiniu.process.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ChangeLifecycle extends Base {

    private BucketManager bucketManager;
    private BatchOperations batchOperations;
    private int days;

    public ChangeLifecycle(String accessKey, String secretKey, Configuration configuration, String bucket, int days,
                           String rmPrefix, String savePath, int saveIndex) throws IOException {
        super("lifecycle", accessKey, secretKey, configuration, bucket, rmPrefix, savePath, saveIndex);
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        this.batchOperations = new BatchOperations();
        this.days = days;
        this.batchSize = 1000;
    }

    public void updateLifecycle(String bucket, int days, String rmPrefix) {
        this.bucket = bucket;
        this.days = days;
        this.rmPrefix = rmPrefix;
    }

    public ChangeLifecycle(String accessKey, String secretKey, Configuration configuration, String bucket, int days,
                           String rmPrefix, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, days, rmPrefix, savePath, 0);
    }

    public ChangeLifecycle clone() throws CloneNotSupportedException {
        ChangeLifecycle changeLifecycle = (ChangeLifecycle)super.clone();
        changeLifecycle.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        if (batchSize > 1) changeLifecycle.batchOperations = new BatchOperations();
        return changeLifecycle;
    }

    @Override
    synchronized protected String batchResult(List<Map<String, String>> lineList) throws QiniuException {
        batchOperations.clearOps();
        lineList.forEach(line -> batchOperations.addDeleteAfterDaysOps(bucket, days, line.get("key")));
        return HttpResponseUtils.getResult(bucketManager.batch(batchOperations));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws QiniuException {
        return HttpResponseUtils.getResult(bucketManager.deleteAfterDays(bucket, line.get("key"), days));
    }
}
