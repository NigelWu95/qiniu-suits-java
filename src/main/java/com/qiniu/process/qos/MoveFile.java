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
import java.util.List;
import java.util.Map;

public class MoveFile extends Base<Map<String, String>> {

    private boolean isRename = false;
    private String toBucket;
    private String toKeyIndex;
    private String addPrefix;
    private String rmPrefix;
    private BatchOperations batchOperations;
    private List<Map<String, String>> lines;
    private Configuration configuration;
    private BucketManager bucketManager;

    public MoveFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String toKeyIndex, String addPrefix, boolean forceIfOnlyPrefix, String rmPrefix) throws IOException {
        // 目标 bucket 为空时规定为 rename 操作
        super(toBucket == null || "".equals(toBucket) ? "rename" : "move", accessKey, secretKey, bucket);
        if ("rename".equals(processName)) isRename = true;
        set(configuration, toBucket, toKeyIndex, addPrefix, forceIfOnlyPrefix, rmPrefix);
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        CloudAPIUtils.checkQiniu(bucketManager, bucket);
        CloudAPIUtils.checkQiniu(bucketManager, toBucket);
    }

    public MoveFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String toKeyIndex, String addPrefix, boolean forceIfOnlyPrefix, String rmPrefix, String savePath,
                    int saveIndex) throws IOException {
        // 目标 bucket 为空时规定为 rename 操作
        super(toBucket == null || "".equals(toBucket) ? "rename" : "move", accessKey, secretKey, bucket, savePath, saveIndex);
        if ("rename".equals(processName)) isRename = true;
        set(configuration, toBucket, toKeyIndex, addPrefix, forceIfOnlyPrefix, rmPrefix);
        this.batchSize = 1000;
        this.batchOperations = new BatchOperations();
        this.lines = new ArrayList<>();
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        CloudAPIUtils.checkQiniu(bucketManager, bucket);
        CloudAPIUtils.checkQiniu(bucketManager, toBucket);
    }

    public MoveFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String toKeyIndex, String keyPrefix, boolean forceIfOnlyPrefix, String rmPrefix, String savePath)
            throws IOException {
        this(accessKey, secretKey, configuration, bucket, toBucket, toKeyIndex, keyPrefix, forceIfOnlyPrefix, rmPrefix,
                savePath, 0);
    }

    private void set(Configuration configuration, String toBucket, String toKeyIndex, String addPrefix,
                     boolean forceIfOnlyPrefix, String rmPrefix) throws IOException {
        this.configuration = configuration;
        this.toBucket = toBucket;
        if (toKeyIndex == null || "".equals(toKeyIndex)) {
            this.toKeyIndex = "key";
            if (toBucket == null || "".equals(toBucket)) {
                // rename 操作时未设置 new-key 的条件判断
                if (forceIfOnlyPrefix) {
                    if (addPrefix == null || "".equals(addPrefix))
                        throw new IOException("although prefix-force is true, but the add-prefix is empty.");
                } else {
                    throw new IOException("there is no to-key index, if you only want to add prefix for renaming, " +
                            "please set the \"prefix-force\" as true.");
                }
            }
        } else {
            this.toKeyIndex = toKeyIndex;
        }
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

    public MoveFile clone() throws CloneNotSupportedException {
        MoveFile moveFile = (MoveFile)super.clone();
        moveFile.bucketManager = new BucketManager(Auth.create(authKey1, authKey2), configuration.clone());
        moveFile.batchOperations = new BatchOperations();
        moveFile.lines = new ArrayList<>();
        return moveFile;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get("key") + "\t" + line.get(toKeyIndex);
    }

    @Override
    protected List<Map<String, String>> putBatchOperations(List<Map<String, String>> processList) throws IOException {
        batchOperations.clearOps();
        lines.clear();
        String key;
        String toKey;
        for (Map<String, String> map : processList) {
            key = map.get("key");
            if (key != null) {
                try {
                    toKey = addPrefix + FileUtils.rmPrefix(rmPrefix, map.get(toKeyIndex));
                    lines.add(map);
                    if (isRename) {
                        batchOperations.addRenameOp(bucket, key, toKey);
                    } else {
                        batchOperations.addMoveOp(bucket, key, toBucket, toKey);
                    }
                } catch (IOException e) {
                    fileSaveMapper.writeError("no " + toKeyIndex + " in " + map, false);
                }
            } else {
                fileSaveMapper.writeError("no key in " + map, false);
            }
        }
        return lines;
    }

    @Override
    protected String batchResult(List<Map<String, String>> lineList) throws IOException {
        if (lineList.size() <= 0) return null;
        return HttpRespUtils.getResult(bucketManager.batch(batchOperations));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws IOException {
        String key = line.get("key");
        if (key == null) throw new IOException("no key in " + line);
        String toKey = addPrefix + FileUtils.rmPrefix(rmPrefix, line.get(toKeyIndex));
        if (isRename) {
            return key + "\t" + toKey + "\t" + HttpRespUtils.getResult(bucketManager.rename(bucket, key, toKey));
        } else {
            return key + "\t" + toKey + "\t" + HttpRespUtils.getResult(bucketManager.move(bucket, key, toBucket, toKey));
        }
    }

    @Override
    public void closeResource() {
        super.closeResource();
        toBucket = null;
        toKeyIndex = null;
        addPrefix = null;
        rmPrefix = null;
        batchOperations = null;
        lines = null;
        configuration = null;
        bucketManager = null;
    }
}
