package com.qiniu.process.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.FileNameUtils;
import com.qiniu.util.HttpResponseUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CopyFile extends Base<Map<String, String>> {

    private String toBucket;
    private String newKeyIndex;
    private String addPrefix;
    private String rmPrefix;
    private BatchOperations batchOperations;
    private BucketManager bucketManager;

    public CopyFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String newKeyIndex, String addPrefix, String rmPrefix, String savePath, int saveIndex) throws IOException {
        super("copy", accessKey, secretKey, configuration, bucket, savePath, saveIndex);
        set(toBucket, newKeyIndex, addPrefix, rmPrefix);
        this.batchSize = 1000;
        this.batchOperations = new BatchOperations();
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
    }

    public void updateCopy(String bucket, String toBucket, String newKeyIndex, String keyPrefix, String rmPrefix) {
        this.bucket = bucket;
        set(toBucket, newKeyIndex, keyPrefix, rmPrefix);
    }

    private void set(String toBucket, String newKeyIndex, String addPrefix, String rmPrefix) {
        this.toBucket = toBucket;
        // 没有传入的 newKeyIndex 参数的话直接设置为默认的 "key"
        this.newKeyIndex = newKeyIndex == null || "".equals(newKeyIndex) ? "key" : newKeyIndex;
        this.addPrefix = addPrefix == null ? "" : addPrefix;
        this.rmPrefix = rmPrefix == null ? "" : rmPrefix;
    }

    public CopyFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String newKeyIndex, String keyPrefix, String rmPrefix, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, toBucket, newKeyIndex, keyPrefix, rmPrefix, savePath, 0);
    }

    public CopyFile clone() throws CloneNotSupportedException {
        CopyFile copyFile = (CopyFile)super.clone();
        copyFile.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        if (batchSize > 1) copyFile.batchOperations = new BatchOperations();
        return copyFile;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get("key") + "\t" + line.get("to-key");
    }

    @Override
    protected boolean checkKeyValid(Map<String, String> line, String key) {
        return line.get(key) == null;
    }

    @Override
    synchronized protected String batchResult(List<Map<String, String>> lineList) throws QiniuException {
        batchOperations.clearOps();
        lineList.forEach(line -> {
            try {
                String toKey = FileNameUtils.rmPrefix(rmPrefix, line.get(newKeyIndex));
                line.put("to-key", addPrefix + toKey);
                batchOperations.addCopyOp(bucket, line.get("key"), toBucket, line.get("to-key"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return HttpResponseUtils.getResult(bucketManager.batch(batchOperations));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws QiniuException {
        try {
            String toKey = FileNameUtils.rmPrefix(rmPrefix, line.get(newKeyIndex));
            line.put("to-key", addPrefix + toKey);
            return HttpResponseUtils.getResult(bucketManager.copy(bucket, line.get("key"), toBucket,
                    line.get("to-key"), false));
        } catch (IOException e) {
            throw new QiniuException(e, e.getMessage());
        }
    }
}
