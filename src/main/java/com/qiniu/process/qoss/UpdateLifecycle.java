package com.qiniu.process.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class UpdateLifecycle extends Base {

    final private int days;
    private BucketManager bucketManager;

    public UpdateLifecycle(String accessKey, String secretKey, Configuration configuration, String bucket, int days,
                           String rmPrefix, String savePath, int saveIndex) throws IOException {
        super("lifecycle", accessKey, secretKey, configuration, bucket, rmPrefix, savePath, saveIndex);
        this.days = days;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration);
        this.batchSize = 1000;
    }

    public UpdateLifecycle(String accessKey, String secretKey, Configuration configuration, String bucket, int days,
                           String rmPrefix, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, days, rmPrefix, savePath, 0);
    }

    public UpdateLifecycle clone() throws CloneNotSupportedException {
        UpdateLifecycle updateLifecycle = (UpdateLifecycle)super.clone();
        updateLifecycle.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        return updateLifecycle;
    }

    protected String batchResult(List<Map<String, String>> lineList) throws QiniuException {
        BucketManager.BatchOperations batchOperations = new BucketManager.BatchOperations();
        lineList.forEach(line -> batchOperations.addDeleteAfterDaysOps(bucket, days, line.get("key")));
        return HttpResponseUtils.getResult(bucketManager.batch(batchOperations));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws QiniuException {
        return HttpResponseUtils.getResult(bucketManager.deleteAfterDays(bucket, line.get("key"), days));
    }
}
