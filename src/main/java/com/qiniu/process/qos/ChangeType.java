package com.qiniu.process.qos;

import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.StorageType;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpRespUtils;
import com.qiniu.util.CloudAPIUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ChangeType extends Base<Map<String, String>> {

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
        changeType.batchOperations = new BatchOperations();
        changeType.errorLineList = new ArrayList<>();
        return changeType;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get("key");
    }

    @Override
    synchronized protected List<Map<String, String>> putBatchOperations(List<Map<String, String>> processList) {
        batchOperations.clearOps();
        Iterator<Map<String, String>> iterator = processList.iterator();
        Map<String, String> line;
        String key;
        while (iterator.hasNext()) {
            line = iterator.next();
            key = line.get("key");
            if (key != null) {
                batchOperations.addChangeTypeOps(bucket, storageType, key);
            } else {
                iterator.remove();
                errorLineList.add("no key in " + line);
            }
        }
        return processList;
    }

    @Override
    protected String batchResult(List<Map<String, String>> lineList) throws IOException {
        return HttpRespUtils.getResult(bucketManager.batch(batchOperations));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws IOException {
        String key = line.get("key");
        if (key == null) throw new IOException("no key in " + line);
        return key + "\t" + storageType + "\t" + HttpRespUtils.getResult(bucketManager.changeType(bucket, key, storageType));
    }

    @Override
    public void closeResource() {
        super.closeResource();
        batchOperations = null;
        configuration = null;
        bucketManager = null;
    }
}
