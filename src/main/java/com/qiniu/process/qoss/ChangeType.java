package com.qiniu.process.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.StorageType;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ChangeType extends Base {

    private BucketManager bucketManager;
    private BatchOperations batchOperations;
    private int type;

    public ChangeType(String accessKey, String secretKey, Configuration configuration, String bucket, int type,
                      String rmPrefix, String savePath, int saveIndex) throws IOException {
        super("type", accessKey, secretKey, configuration, bucket, rmPrefix, savePath, saveIndex);
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        this.batchOperations = new BatchOperations();
        this.type = type;
        this.batchSize = 1000;
    }

    public void updateType(String bucket, int type, String rmPrefix) {
        this.bucket = bucket;
        this.type = type;
        this.rmPrefix = rmPrefix;
    }

    public ChangeType(String accessKey, String secretKey, Configuration configuration, String bucket, int type,
                      String rmPrefix, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, type, rmPrefix, savePath, 0);
    }

    public ChangeType clone() throws CloneNotSupportedException {
        ChangeType changeType = (ChangeType)super.clone();
        changeType.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        if (batchSize > 1) changeType.batchOperations = new BatchOperations();
        return changeType;
    }

    @Override
    synchronized protected String batchResult(List<Map<String, String>> lineList) throws QiniuException {
        batchOperations.clearOps();
        lineList.forEach(line -> batchOperations.addChangeTypeOps(bucket, type == 0 ? StorageType.COMMON :
                StorageType.INFREQUENCY, line.get("key")));
        return HttpResponseUtils.getResult(bucketManager.batch(batchOperations));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws QiniuException {
        return HttpResponseUtils.getResult(bucketManager.changeType(bucket, line.get("key"), type == 0 ?
                StorageType.COMMON : StorageType.INFREQUENCY));
    }
}
