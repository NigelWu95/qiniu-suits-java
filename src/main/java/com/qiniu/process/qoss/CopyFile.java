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

public class CopyFile extends Base {

    private String toBucket;
    private String newKeyIndex;
    private String keyPrefix;
    private BatchOperations batchOperations;
    private BucketManager bucketManager;

    public CopyFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String newKeyIndex, String keyPrefix, String rmPrefix, String savePath, int saveIndex) throws IOException {
        super("copy", accessKey, secretKey, configuration, bucket, rmPrefix, savePath, saveIndex);
        set(toBucket, newKeyIndex, keyPrefix);
        this.batchSize = 1000;
        this.batchOperations = new BatchOperations();
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
    }

    public void updateCopy(String bucket, String toBucket, String newKeyIndex, String keyPrefix, String rmPrefix) {
        this.bucket = bucket;
        set(toBucket, newKeyIndex, keyPrefix);
        this.rmPrefix = rmPrefix;
    }

    private void set(String toBucket, String newKeyIndex, String keyPrefix) {
        this.toBucket = toBucket;
        // 没有传入的 newKeyIndex 参数的话直接设置为默认的 "key"
        this.newKeyIndex = newKeyIndex == null || "".equals(newKeyIndex) ? "key" : newKeyIndex;
        this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
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
        return line.get("key") + "\t" + line.get(newKeyIndex);
    }

    @Override
    synchronized protected String batchResult(List<Map<String, String>> lineList) throws QiniuException {
        batchOperations.clearOps();
        lineList.forEach(line -> batchOperations.addCopyOp(bucket, line.get("key"), toBucket,
                keyPrefix + line.get(newKeyIndex)));
        return HttpResponseUtils.getResult(bucketManager.batch(batchOperations));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws QiniuException {
        return HttpResponseUtils.getResult(
                bucketManager.copy(bucket, line.get("key"), toBucket, keyPrefix + line.get(newKeyIndex), false));
    }
}
