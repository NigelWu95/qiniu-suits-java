package com.qiniu.process.qos;

import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpRespUtils;
import com.qiniu.util.CloudAPIUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ChangeStatus extends Base<Map<String, String>> {

    private int status;
    private BatchOperations batchOperations;
    private List<String> errorLineList;
    private Configuration configuration;
    private BucketManager bucketManager;

    public ChangeStatus(String accessKey, String secretKey, Configuration configuration, String bucket, int status)
            throws IOException {
        super("status", accessKey, secretKey, bucket);
        this.status = status;
        this.configuration = configuration;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        CloudAPIUtils.checkQiniu(bucketManager, bucket);
    }

    public ChangeStatus(String accessKey, String secretKey, Configuration configuration, String bucket, int status,
                        String savePath, int saveIndex) throws IOException {
        super("status", accessKey, secretKey, bucket, savePath, saveIndex);
        this.status = status;
        this.batchSize = 1000;
        this.batchOperations = new BatchOperations();
        this.errorLineList = new ArrayList<>();
        this.configuration = configuration;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        CloudAPIUtils.checkQiniu(bucketManager, bucket);
    }

    public ChangeStatus(String accessKey, String secretKey, Configuration configuration, String bucket, int status,
                        String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, status, savePath, 0);
    }

    public void updateStatus(int status) {
        this.status = status;
    }

    public ChangeStatus clone() throws CloneNotSupportedException {
        ChangeStatus changeStatus = (ChangeStatus)super.clone();
        changeStatus.bucketManager = new BucketManager(Auth.create(authKey1, authKey2), configuration.clone());
        changeStatus.batchOperations = new BatchOperations();
        changeStatus.errorLineList = new ArrayList<>();
        return changeStatus;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get("key");
    }

    @Override
    synchronized protected List<Map<String, String>> putBatchOperations(List<Map<String, String>> processList) throws IOException {
        batchOperations.clearOps();
        Iterator<Map<String, String>> iterator = processList.iterator();
        Map<String, String> line;
        String key;
        while (iterator.hasNext()) {
            line = iterator.next();
            key = line.get("key");
            if (key != null) {
                batchOperations.addChangeStatusOps(bucket, status, key);
            } else {
                iterator.remove();
                errorLineList.add("no key in " + line);
            }
        }
        if (errorLineList.size() > 0) {
            fileSaveMapper.writeError(String.join("\n", errorLineList), false);
            errorLineList.clear();
        }
        return processList;
    }

    @Override
    protected String batchResult(List<Map<String, String>> lineList) throws IOException {
        return HttpRespUtils.getResult(bucketManager.batch(batchOperations));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws IOException {
        String key = line.get("key");
        if (key == null) throw new IOException("no key in " + line);
        return key + "\t" + status + "\t" + HttpRespUtils.getResult(bucketManager.changeStatus(bucket, key, status));
    }

    @Override
    public void closeResource() {
        super.closeResource();
        batchOperations = null;
        errorLineList = null;
        configuration = null;
        bucketManager = null;
    }
}
