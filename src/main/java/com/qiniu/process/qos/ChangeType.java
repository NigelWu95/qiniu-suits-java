package com.qiniu.process.qos;

import com.qiniu.common.QiniuException;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.StorageType;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpRespUtils;
import com.qiniu.util.CloudAPIUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ChangeType extends Base<Map<String, String>> {

    private int type;
    private StorageType storageType;
    private BatchOperations batchOperations;
    private Configuration configuration;
    private BucketManager bucketManager;

    public ChangeType(String accessKey, String secretKey, Configuration configuration, String bucket, int type)
            throws IOException {
        super("type", accessKey, secretKey, bucket);
        storageType = type == 0 ? StorageType.COMMON : StorageType.INFREQUENCY;
        this.configuration = configuration;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        CloudAPIUtils.checkQiniu(bucketManager, bucket);
    }

    public ChangeType(String accessKey, String secretKey, Configuration configuration, String bucket, int type,
                      String savePath, int saveIndex) throws IOException {
        super("type", accessKey, secretKey, bucket, savePath, saveIndex);
        storageType = type == 0 ? StorageType.COMMON : StorageType.INFREQUENCY;
        this.batchSize = 1000;
        this.batchOperations = new BatchOperations();
        this.configuration = configuration;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        CloudAPIUtils.checkQiniu(bucketManager, bucket);
    }

    public ChangeType(String accessKey, String secretKey, Configuration configuration, String bucket, int type,
                      String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, type, savePath, 0);
    }

    public void updateType(int type) {
        storageType = type == 0 ? StorageType.COMMON : StorageType.INFREQUENCY;
    }

    public ChangeType clone() throws CloneNotSupportedException {
        ChangeType changeType = (ChangeType)super.clone();
        changeType.bucketManager = new BucketManager(Auth.create(authKey1, authKey2), configuration.clone());
        if (batchSize > 1) changeType.batchOperations = new BatchOperations();
        return changeType;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get("key");
    }

    @Override
    protected boolean validCheck(Map<String, String> line) {
        return line.get("key") != null;
    }

    @Override
    synchronized protected String batchResult(List<Map<String, String>> lineList) throws QiniuException {
        batchOperations.clearOps();
        lineList.forEach(line -> batchOperations.addChangeTypeOps(bucket, storageType, line.get("key")));
        return HttpRespUtils.getResult(bucketManager.batch(batchOperations));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws QiniuException {
        String key = line.get("key");
        return key + "\t" + type + "\t" + HttpRespUtils.getResult(bucketManager.changeType(bucket, key, storageType));
    }

    @Override
    public void closeResource() {
        super.closeResource();
        batchOperations = null;
        configuration = null;
        bucketManager = null;
    }
}
