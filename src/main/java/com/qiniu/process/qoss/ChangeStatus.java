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

public class ChangeStatus extends Base<Map<String, String>> {

    private int status;
    private BatchOperations batchOperations;
    private BucketManager bucketManager;

    public ChangeStatus(String accessKey, String secretKey, Configuration configuration, String bucket, int status,
                        String savePath, int saveIndex) throws IOException {
        super("status", accessKey, secretKey, configuration, bucket, savePath, saveIndex);
        this.status = status;
        this.batchSize = 1000;
        this.batchOperations = new BatchOperations();
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
    }

    public void updateStatus(String bucket, int status) {
        this.bucket = bucket;
        this.status = status;
    }

    public ChangeStatus(String accessKey, String secretKey, Configuration configuration, String bucket, int status,
                        String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, status, savePath, 0);
    }

    public ChangeStatus clone() throws CloneNotSupportedException {
        ChangeStatus changeStatus = (ChangeStatus)super.clone();
        changeStatus.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        if (batchSize > 1) changeStatus.batchOperations = new BatchOperations();
        return changeStatus;
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
        lineList.forEach(line -> batchOperations.addChangeStatusOps(bucket, status, line.get("key")));
        return HttpResponseUtils.getResult(bucketManager.batch(batchOperations));
    }

    @Override
    public String singleResult(Map<String, String> line) throws QiniuException {
        return HttpResponseUtils.getResult(bucketManager.changeStatus(bucket, line.get("key"), status));
    }
}
