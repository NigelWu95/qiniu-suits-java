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

public class MoveFile extends Base {

    private BucketManager bucketManager;
    private BatchOperations batchOperations;
    private String toBucket;
    private String newKeyIndex;
    private String keyPrefix;

    public MoveFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String newKeyIndex, String keyPrefix, boolean forceIfOnlyPrefix, String rmPrefix, String savePath,
                    int saveIndex) throws IOException {
        // 目标 bucket 为空时规定为 rename 操作
        super(toBucket == null || "".equals(toBucket) ? "rename" : "move", accessKey, secretKey, configuration, bucket,
                rmPrefix, savePath, saveIndex);
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        this.batchOperations = new BatchOperations();
        set(toBucket, newKeyIndex, keyPrefix, forceIfOnlyPrefix);
        this.batchSize = 1000;
    }

    public void updateMove(String bucket, String toBucket, String newKeyIndex, String keyPrefix,
                           boolean forceIfOnlyPrefix, String rmPrefix) throws IOException {
        this.bucket = bucket;
        set(toBucket, newKeyIndex, keyPrefix, forceIfOnlyPrefix);
        this.rmPrefix = rmPrefix;
    }

    private void set(String toBucket, String newKeyIndex, String keyPrefix, boolean forceIfOnlyPrefix) throws IOException {
        this.toBucket = toBucket;
        if (newKeyIndex == null || "".equals(newKeyIndex)) {
            this.newKeyIndex = "key";
            if (toBucket == null || "".equals(toBucket)) {
                // rename 操作时未设置 new-key 的条件判断
                if (forceIfOnlyPrefix) {
                    if (keyPrefix == null || "".equals(keyPrefix))
                        throw new IOException("although prefix-force is true, but the add-prefix is empty.");
                } else {
                    throw new IOException("there is no newKey index, if you only want to add prefix for renaming, " +
                            "please set the \"prefix-force\" as true.");
                }
            }
        } else {
            this.newKeyIndex = newKeyIndex;
        }
        this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
    }

    public MoveFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String newKeyIndex, String keyPrefix, boolean forceIfOnlyPrefix, String rmPrefix, String savePath)
            throws IOException {
        this(accessKey, secretKey, configuration, bucket, toBucket, newKeyIndex, keyPrefix, forceIfOnlyPrefix, rmPrefix,
                savePath, 0);
    }

    public MoveFile clone() throws CloneNotSupportedException {
        MoveFile moveFile = (MoveFile)super.clone();
        moveFile.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        if (batchSize > 1) moveFile.batchOperations = new BatchOperations();
        return moveFile;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get("key") + "\t" + line.get(newKeyIndex);
    }

    @Override
    protected String batchResult(List<Map<String, String>> lineList) throws QiniuException {
        batchOperations.clearOps();
        lineList.forEach(line -> {
            if (toBucket == null || "".equals(toBucket)) {
                batchOperations.addRenameOp(bucket, line.get("key"), keyPrefix + line.get(newKeyIndex));
            } else {
                batchOperations.addMoveOp(bucket, line.get("key"), toBucket, keyPrefix + line.get(newKeyIndex));
            }
        });
        return HttpResponseUtils.getResult(bucketManager.batch(batchOperations));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws QiniuException {
        if (toBucket == null || "".equals(toBucket)) {
            return HttpResponseUtils.getResult(bucketManager.rename(bucket, line.get("key"),
                    keyPrefix + line.get(newKeyIndex)));
        } else {
            return HttpResponseUtils.getResult(bucketManager.move(bucket, line.get("key"), toBucket,
                    keyPrefix + line.get(newKeyIndex)));
        }
    }
}
