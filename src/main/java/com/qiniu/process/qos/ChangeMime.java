package com.qiniu.process.qos;

import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.CloudApiUtils;
import com.qiniu.util.HttpRespUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ChangeMime extends Base<Map<String, String>> {

    private String mimeType;
    private String mimeIndex;
    private String condition;
    private BucketManager.BatchOperations batchOperations;
    private List<Map<String, String>> lines;
    private Configuration configuration;
    private BucketManager bucketManager;

    public ChangeMime(String accessKey, String secretKey, Configuration configuration, String bucket, String mimeType,
                      String mimeIndex, String condition) throws IOException {
        super("mime", accessKey, secretKey, bucket);
        this.mimeType = mimeType;
        this.mimeIndex = mimeIndex == null ? "mime" : mimeIndex;
        this.configuration = configuration;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        CloudApiUtils.checkQiniu(bucketManager, bucket);
    }

    public ChangeMime(String accessKey, String secretKey, Configuration configuration, String bucket, String mimeType,
                      String mimeIndex, String condition, String savePath, int saveIndex) throws IOException {
        super("mime", accessKey, secretKey, bucket, savePath, saveIndex);
        this.mimeType = mimeType;
        this.mimeIndex = mimeIndex == null ? "mime" : mimeIndex;
        this.batchSize = 1000;
        this.batchOperations = new BucketManager.BatchOperations();
        this.lines = new ArrayList<>();
        this.configuration = configuration;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        CloudApiUtils.checkQiniu(bucketManager, bucket);
    }

    public ChangeMime(String accessKey, String secretKey, Configuration configuration, String bucket, String mimeType,
                      String mimeIndex, String condition, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, mimeType, mimeIndex, condition, savePath, 0);
    }

    public ChangeMime clone() throws CloneNotSupportedException {
        ChangeMime changeType = (ChangeMime)super.clone();
        changeType.bucketManager = new BucketManager(Auth.create(accessId, secretKey), configuration.clone());
        changeType.batchOperations = new BucketManager.BatchOperations();
        changeType.lines = new ArrayList<>();
        return changeType;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get("key");
    }

    @Override
    protected List<Map<String, String>> putBatchOperations(List<Map<String, String>> processList) throws IOException {
        batchOperations.clearOps();
        lines.clear();
        String key;
        if (mimeType == null) {
            String mime;
            for (Map<String, String> map : processList) {
                key = map.get("key");
                mime = map.get(mimeIndex);
                if (key != null && mime != null) {
                    lines.add(map);
                    batchOperations.addChgmOp(bucket, mime, key);
                } else {
                    fileSaveMapper.writeError("key or mime is not exists or empty in " + map, false);
                }
            }
        } else {
            for (Map<String, String> map : processList) {
                key = map.get("key");
                if (key != null) {
                    lines.add(map);
                    batchOperations.addChgmOp(bucket, mimeType, key);
                } else {
                    fileSaveMapper.writeError("key is not exists or empty in " + map, false);
                }
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
        if (mimeType == null) {
            String mime = line.get(mimeIndex);
            if (mime == null) throw new IOException("mime is not exists or empty in " + line);
            return String.join("\t", key, mime, HttpRespUtils.getResult(bucketManager.changeMime(bucket, key, mime)));
        } else {
            return String.join("\t", key, mimeType, HttpRespUtils.getResult(bucketManager.changeMime(bucket, key, mimeType)));
        }
    }

    @Override
    public void closeResource() {
        super.closeResource();
        mimeType = null;
        batchOperations = null;
        lines = null;
        configuration = null;
        bucketManager = null;
    }
}
