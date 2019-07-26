package com.qiniu.process.qos;

import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.FileUtils;
import com.qiniu.util.HttpRespUtils;
import com.qiniu.util.CloudAPIUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CopyFile extends Base<Map<String, String>> {

    private String toBucket;
    private String toKeyIndex;
    private String addPrefix;
    private String rmPrefix;
    private BatchOperations batchOperations;
    private Configuration configuration;
    private BucketManager bucketManager;

    public CopyFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String toKeyIndex, String addPrefix, String rmPrefix) throws IOException {
        super("copy", accessKey, secretKey, bucket);
        set(configuration, toBucket, toKeyIndex, addPrefix, rmPrefix);
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        CloudAPIUtils.checkQiniu(bucketManager, bucket);
        CloudAPIUtils.checkQiniu(bucketManager, toBucket);
    }

    public CopyFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String toKeyIndex, String addPrefix, String rmPrefix, String savePath, int saveIndex) throws IOException {
        super("copy", accessKey, secretKey, bucket, savePath, saveIndex);
        set(configuration, toBucket, toKeyIndex, addPrefix, rmPrefix);
        this.batchSize = 1000;
        this.batchOperations = new BatchOperations();
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        CloudAPIUtils.checkQiniu(bucketManager, bucket);
        CloudAPIUtils.checkQiniu(bucketManager, toBucket);
    }

    public CopyFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String toKeyIndex, String keyPrefix, String rmPrefix, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, toBucket, toKeyIndex, keyPrefix, rmPrefix, savePath, 0);
    }

    private void set(Configuration configuration, String toBucket, String toKeyIndex, String addPrefix, String rmPrefix) {
        this.configuration = configuration;
        this.toBucket = toBucket;
        // 没有传入的 toKeyIndex 参数的话直接设置为默认的 "key"
        this.toKeyIndex = toKeyIndex == null || "".equals(toKeyIndex) ? "key" : toKeyIndex;
        this.addPrefix = addPrefix == null ? "" : addPrefix;
        this.rmPrefix = rmPrefix == null ? "" : rmPrefix;
    }

    public void updateToBucket(String toBucket) {
        this.toBucket = toBucket;
    }

    public void updateToKeyIndex(String toKeyIndex) {
        this.toKeyIndex = toKeyIndex;
    }

    public void updateAddPrefix(String addPrefix) {
        this.addPrefix = addPrefix;
    }

    public void updateRmPrefix(String rmPrefix) {
        this.rmPrefix = rmPrefix;
    }

    public CopyFile clone() throws CloneNotSupportedException {
        CopyFile copyFile = (CopyFile)super.clone();
        copyFile.bucketManager = new BucketManager(Auth.create(authKey1, authKey2), configuration.clone());
        copyFile.batchOperations = new BatchOperations();
        copyFile.errorLineList = new ArrayList<>();
        return copyFile;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get("key") + "\t" + line.get(toKeyIndex);
    }

    @Override
    synchronized protected List<Map<String, String>> putBatchOperations(List<Map<String, String>> processList) {
        batchOperations.clearOps();
        Iterator<Map<String, String>> iterator = processList.iterator();
        Map<String, String> line;
        String key;
        String toKey;
        while (iterator.hasNext()) {
            line = iterator.next();
            key = line.get("key");
            if (key != null) {
                try {
                    toKey = addPrefix + FileUtils.rmPrefix(rmPrefix, line.get(toKeyIndex));
                    batchOperations.addCopyOp(bucket, key, toBucket, toKey);
                } catch (IOException e) {
                    iterator.remove();
                    errorLineList.add("no " + toKeyIndex + " in " + line);
                }
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
        String toKey = line.get(toKeyIndex);
        if (key == null || toKey == null) throw new IOException("no key or to-key in " + line);
        return key + "\t" + toKey + "\t" + HttpRespUtils.getResult(bucketManager.copy(bucket, key, toBucket, toKey, false));
    }

    @Override
    public void closeResource() {
        super.closeResource();
        toBucket = null;
        toKeyIndex = null;
        addPrefix = null;
        rmPrefix = null;
        batchOperations = null;
        configuration = null;
        bucketManager = null;
    }
}
