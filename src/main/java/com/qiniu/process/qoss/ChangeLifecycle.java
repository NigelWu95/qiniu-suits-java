package com.qiniu.process.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpRespUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ChangeLifecycle extends Base<Map<String, String>> {

    private int days;
    private BatchOperations batchOperations;
    private BucketManager bucketManager;

    public ChangeLifecycle(String accessKey, String secretKey, Configuration configuration, String bucket, int days)
            throws IOException {
        super("lifecycle", accessKey, secretKey, configuration, bucket);
        this.days = days;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
    }

    public ChangeLifecycle(String accessKey, String secretKey, Configuration configuration, String bucket, int days,
                           String savePath, int saveIndex) throws IOException {
        super("lifecycle", accessKey, secretKey, configuration, bucket, savePath, saveIndex);
        this.days = days;
        this.batchSize = 1000;
        this.batchOperations = new BatchOperations();
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
    }

    public ChangeLifecycle(String accessKey, String secretKey, Configuration configuration, String bucket, int days,
                           String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, days, savePath, 0);
    }

    public void updateLifecycle(String bucket, int days) {
        this.bucket = bucket;
        this.days = days;
    }

    public ChangeLifecycle clone() throws CloneNotSupportedException {
        ChangeLifecycle changeLifecycle = (ChangeLifecycle)super.clone();
        changeLifecycle.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        if (batchSize > 1) changeLifecycle.batchOperations = new BatchOperations();
        return changeLifecycle;
    }

    @Override
    public String resultInfo(Map<String, String> line) {
        return line.get("key");
    }

    @Override
    public boolean validCheck(Map<String, String> line) {
        return line.get("key") != null;
    }

    @Override
    synchronized public String batchResult(List<Map<String, String>> lineList) throws QiniuException {
        batchOperations.clearOps();
        lineList.forEach(line -> batchOperations.addDeleteAfterDaysOps(bucket, days, line.get("key")));
        return HttpRespUtils.getResult(bucketManager.batch(batchOperations));
    }

    @Override
    public String singleResult(Map<String, String> line) throws QiniuException {
        String key = line.get("key");
        return key + "\t" + HttpRespUtils.getResult(bucketManager.deleteAfterDays(bucket, key, days));
    }
}
