package com.qiniu.process.qoss;

import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.FileNameUtils;
import com.qiniu.util.HttpRespUtils;

import java.io.IOException;
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
                    String toKeyIndex, String addPrefix, String rmPrefix) {
        super("copy", accessKey, secretKey, bucket);
        set(configuration, toBucket, toKeyIndex, addPrefix, rmPrefix);
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
    }

    public CopyFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String toKeyIndex, String addPrefix, String rmPrefix, String savePath, int saveIndex) throws IOException {
        super("copy", accessKey, secretKey, bucket, savePath, saveIndex);
        set(configuration, toBucket, toKeyIndex, addPrefix, rmPrefix);
        this.batchSize = 1000;
        this.batchOperations = new BatchOperations();
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
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

    public CopyFile clone() throws CloneNotSupportedException {
        CopyFile copyFile = (CopyFile)super.clone();
        copyFile.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        if (batchSize > 1) copyFile.batchOperations = new BatchOperations();
        return copyFile;
    }

    @Override
    public String resultInfo(Map<String, String> line) {
        return line.get("key") + "\t" + line.get("to-key");
    }

    @Override
    public boolean validCheck(Map<String, String> line) {
        if (line.get("key") == null) return false;
        try {
            String toKey = FileNameUtils.rmPrefix(rmPrefix, line.get(toKeyIndex));
            line.put("to-key", addPrefix + toKey);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    synchronized public String batchResult(List<Map<String, String>> lineList) throws IOException {
        batchOperations.clearOps();
        lineList.forEach(line -> batchOperations.addCopyOp(bucket, line.get("key"), toBucket, line.get("to-key")));
        return HttpRespUtils.getResult(bucketManager.batch(batchOperations));
    }

    @Override
    public String singleResult(Map<String, String> line) throws IOException {
        String key = line.get("key");
        String toKey = line.get("to-key");
        return key + "\t" + toKey + "\t" + HttpRespUtils.getResult(bucketManager.copy(bucket, key, toBucket, toKey, false));
    }
}
