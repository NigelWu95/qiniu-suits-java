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

public class MoveFile extends Base<Map<String, String>> {

    private String toBucket;
    private String newKeyIndex;
    private String addPrefix;
    private String rmPrefix;
    private BatchOperations batchOperations;
    private BucketManager bucketManager;

    public MoveFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String newKeyIndex, String addPrefix, boolean forceIfOnlyPrefix, String rmPrefix, String savePath,
                    int saveIndex) throws IOException {
        // 目标 bucket 为空时规定为 rename 操作
        super(toBucket == null || "".equals(toBucket) ? "rename" : "move", accessKey, secretKey, configuration, bucket,
                savePath, saveIndex);
        set(toBucket, newKeyIndex, addPrefix, forceIfOnlyPrefix, rmPrefix);
        this.batchSize = 1000;
        this.batchOperations = new BatchOperations();
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
    }

    public void updateMove(String bucket, String toBucket, String newKeyIndex, String addPrefix,
                           boolean forceIfOnlyPrefix, String rmPrefix) throws IOException {
        this.bucket = bucket;
        set(toBucket, newKeyIndex, addPrefix, forceIfOnlyPrefix, rmPrefix);
    }

    private void set(String toBucket, String newKeyIndex, String addPrefix, boolean forceIfOnlyPrefix, String rmPrefix)
            throws IOException {
        this.toBucket = toBucket;
        if (newKeyIndex == null || "".equals(newKeyIndex)) {
            this.newKeyIndex = "key";
            if (toBucket == null || "".equals(toBucket)) {
                // rename 操作时未设置 new-key 的条件判断
                if (forceIfOnlyPrefix) {
                    if (addPrefix == null || "".equals(addPrefix))
                        throw new IOException("although prefix-force is true, but the add-prefix is empty.");
                } else {
                    throw new IOException("there is no newKey index, if you only want to add prefix for renaming, " +
                            "please set the \"prefix-force\" as true.");
                }
            }
        } else {
            this.newKeyIndex = newKeyIndex;
        }
        this.addPrefix = addPrefix == null ? "" : addPrefix;
        this.rmPrefix = rmPrefix == null ? "" : rmPrefix;
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
                line.put("to-key", toKey);
                if (toBucket == null || "".equals(toBucket)) {
                    batchOperations.addRenameOp(bucket, line.get("key"), addPrefix + toKey);
                } else {
                    batchOperations.addMoveOp(bucket, line.get("key"), toBucket, addPrefix + toKey);
                }
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
            line.put("to-key", toKey);
            if (toBucket == null || "".equals(toBucket)) {
                return HttpResponseUtils.getResult(bucketManager.rename(bucket, line.get("key"), line.get("to-key")));
            } else {
                return HttpResponseUtils.getResult(bucketManager.move(bucket, line.get("key"), toBucket, line.get("to-key")));
            }
        } catch (IOException e) {
            throw new QiniuException(e, e.getMessage());
        }
    }
}
