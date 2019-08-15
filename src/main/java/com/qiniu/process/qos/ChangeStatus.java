package com.qiniu.process.qos;

import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpRespUtils;
import com.qiniu.util.CloudApiUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ChangeStatus extends Base<Map<String, String>> {

    private int status;
    private BatchOperations batchOperations;
    private List<Map<String, String>> lines;
    private Configuration configuration;
    private BucketManager bucketManager;

    public ChangeStatus(String accessKey, String secretKey, Configuration configuration, String bucket, int status)
            throws IOException {
        super("status", accessKey, secretKey, bucket);
        this.status = status;
        this.configuration = configuration;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        CloudApiUtils.checkQiniu(bucketManager, bucket);
    }

    public ChangeStatus(String accessKey, String secretKey, Configuration configuration, String bucket, int status,
                        String savePath, int saveIndex) throws IOException {
        super("status", accessKey, secretKey, bucket, savePath, saveIndex);
        this.status = status;
        this.batchSize = 1000;
        this.batchOperations = new BatchOperations();
        this.lines = new ArrayList<>();
        this.configuration = configuration;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        CloudApiUtils.checkQiniu(bucketManager, bucket);
    }

    public ChangeStatus(String accessKey, String secretKey, Configuration configuration, String bucket, int status,
                        String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, status, savePath, 0);
    }

    public ChangeStatus clone() throws CloneNotSupportedException {
        ChangeStatus changeStatus = (ChangeStatus)super.clone();
        changeStatus.bucketManager = new BucketManager(Auth.create(accessId, secretKey), configuration.clone());
        changeStatus.batchOperations = new BatchOperations();
        changeStatus.lines = new ArrayList<>();
        return changeStatus;
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
        for (Map<String, String> map : processList) {
            key = map.get("key");
            if (key != null) {
                lines.add(map);
                batchOperations.addChangeStatusOps(bucket, status, key);
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
        return key + "\t" + status + "\t" + HttpRespUtils.getResult(bucketManager.changeStatus(bucket, key, status));
    }

    @Override
    public void closeResource() {
        super.closeResource();
        batchOperations = null;
        lines = null;
        configuration = null;
        bucketManager = null;
    }
}
