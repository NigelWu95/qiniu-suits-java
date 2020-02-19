package com.qiniu.process.qiniu;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.FileUtils;
import com.qiniu.util.HttpRespUtils;
import com.qiniu.util.CloudApiUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CopyFile extends Base<Map<String, String>> {

    private String toBucket;
    private String toKeyIndex;
    private String addPrefix;
    private String rmPrefix;
    private boolean defaultToKey = false;
    private BatchOperations batchOperations;
    private List<Map<String, String>> lines;
    private Configuration configuration;
    private BucketManager bucketManager;

    public CopyFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String toKeyIndex, String addPrefix, String rmPrefix) throws IOException {
        super("copy", accessKey, secretKey, bucket);
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        CloudApiUtils.checkQiniu(bucketManager, bucket);
        CloudApiUtils.checkQiniu(bucketManager, toBucket);
        set(configuration, toBucket, toKeyIndex, addPrefix, rmPrefix);
    }

    public CopyFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String toKeyIndex, String addPrefix, String rmPrefix, String savePath, int saveIndex) throws IOException {
        super("copy", accessKey, secretKey, bucket, savePath, saveIndex);
        this.batchSize = 1000;
        this.batchOperations = new BatchOperations();
        this.lines = new ArrayList<>();
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        CloudApiUtils.checkQiniu(bucketManager, bucket);
        CloudApiUtils.checkQiniu(bucketManager, toBucket);
        set(configuration, toBucket, toKeyIndex, addPrefix, rmPrefix);
    }

    public CopyFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String toKeyIndex, String keyPrefix, String rmPrefix, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, toBucket, toKeyIndex, keyPrefix, rmPrefix, savePath, 0);
    }

    private void set(Configuration configuration, String toBucket, String toKeyIndex, String addPrefix, String rmPrefix)
            throws IOException {
        this.configuration = configuration;
        this.toBucket = toBucket;
        this.toKeyIndex = toKeyIndex;
        this.addPrefix = addPrefix == null ? "" : addPrefix;
        this.rmPrefix = rmPrefix;
        if (toKeyIndex == null || "".equals(toKeyIndex)) {
            this.toKeyIndex = "toKey"; // 没有传入的 toKeyIndex 参数的话直接设置为默认的 "toKey"
            defaultToKey = true;
            if (bucket.equals(toBucket) && (addPrefix == null || "".equals(addPrefix)) && (rmPrefix == null || "".equals(rmPrefix))) {
                throw new IOException("toBucket is same as bucket, but no toKeyIndex and no valid addPrefix or rmPrefix.");
            }
        }
    }

    @Override
    public CopyFile clone() throws CloneNotSupportedException {
        CopyFile copyFile = (CopyFile)super.clone();
        copyFile.bucketManager = new BucketManager(Auth.create(accessId, secretKey), configuration.clone());
        copyFile.batchOperations = new BatchOperations();
        copyFile.lines = new ArrayList<>();
        return copyFile;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return String.join("\t", line.get("key"), line.get(toKeyIndex));
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
                    if (defaultToKey) {
                        toKey = String.join("", addPrefix, FileUtils.rmPrefix(rmPrefix, key));
                    } else {
                        toKey = String.join("", addPrefix, FileUtils.rmPrefix(rmPrefix, map.get(toKeyIndex)));
                    }
                    map.put(toKeyIndex, toKey);
                    lines.add(map);
                    batchOperations.addCopyOp(bucket, key, toBucket, toKey);
                } catch (IOException e) {
                    fileSaveMapper.writeError("no " + toKeyIndex + " in " + map, false);
                }
            } else {
                fileSaveMapper.writeError("key is not exists or empty in " + map, false);
            }
        }
        return lines;
    }

    @Override
    protected String batchResult(List<Map<String, String>> lineList) throws IOException {
        return HttpRespUtils.getResult(bucketManager.batch(batchOperations));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws IOException {
        String key = line.get("key");
        if (key == null) throw new IOException("key is not exists or empty in " + line);
        String toKey = addPrefix + FileUtils.rmPrefix(rmPrefix, defaultToKey ? key : line.get(toKeyIndex));
        Response response = bucketManager.copy(bucket, key, toBucket, toKey, false);
        if (response.statusCode != 200) throw new QiniuException(response);
        response.close();
        return String.join("\t", key, toKey, "200");
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
