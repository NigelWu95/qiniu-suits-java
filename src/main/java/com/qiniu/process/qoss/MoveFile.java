package com.qiniu.process.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MoveFile extends Base {

    final private String toBucket;
    final private String newKeyIndex;
    final private String keyPrefix;
    private BucketManager bucketManager;

    public MoveFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String newKeyIndex, String keyPrefix, String rmPrefix, boolean forceIfOnlyPrefix, String savePath,
                    int saveIndex) throws IOException {
        // 目标 bucket 为空时规定为 rename 操作
        super(toBucket == null || "".equals(toBucket) ? "rename" : "move", accessKey, secretKey, configuration, bucket,
                rmPrefix, savePath, saveIndex);
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
        this.toBucket = toBucket;
        this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration);
        this.batchSize = 1000;
    }

    public MoveFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String newKeyIndex, String keyPrefix, String rmPrefix, boolean forceIfOnlyPrefix, String savePath)
            throws IOException {
        this(accessKey, secretKey, configuration, bucket, toBucket, newKeyIndex, keyPrefix, rmPrefix, forceIfOnlyPrefix,
                savePath, 0);
    }

    public MoveFile clone() throws CloneNotSupportedException {
        MoveFile moveFile = (MoveFile)super.clone();
        moveFile.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        return moveFile;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get("key") + "\t" + line.get(newKeyIndex);
    }

    protected Response batchResult(List<Map<String, String>> lineList) throws QiniuException {
        BatchOperations batchOperations = new BucketManager.BatchOperations();
        lineList.forEach(line -> {
            if (toBucket == null || "".equals(toBucket)) {
                batchOperations.addRenameOp(bucket, line.get("key"), keyPrefix + line.get(newKeyIndex));
            } else {
                batchOperations.addMoveOp(bucket, line.get("key"), toBucket, keyPrefix + line.get(newKeyIndex));
            }
        });
        return bucketManager.batch(batchOperations);
    }
}
