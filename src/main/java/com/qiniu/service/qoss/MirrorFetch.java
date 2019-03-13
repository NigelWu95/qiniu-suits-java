package com.qiniu.service.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.persistence.FileMap;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MirrorFetch implements ILineProcess<Map<String, String>>, Cloneable {

    final protected String accessKey;
    final protected String secretKey;
    final protected Configuration configuration;
    protected BucketManager bucketManager;
    final protected String bucket;
    final protected String processName;
    protected int retryCount;
    final protected String savePath;
    protected String saveTag;
    protected int saveIndex;
    protected FileMap fileMap;

    public MirrorFetch(String accessKey, String secretKey, Configuration configuration,
                         String bucket, String savePath, int saveIndex) throws IOException {
        this.processName = "mirror";
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.configuration = configuration;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration);
        this.bucket = bucket;
        this.savePath = savePath;
        this.saveTag = "";
        this.saveIndex = saveIndex;
        this.fileMap = new FileMap(savePath, processName, String.valueOf(saveIndex));
        this.fileMap.initDefaultWriters();
    }

    public MirrorFetch(String accessKey, String secretKey, Configuration configuration, String bucket, String savePath)
            throws IOException {
        this(accessKey, secretKey, configuration, bucket, savePath, 0);
    }

    public String getProcessName() {
        return this.processName;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount < 1 ? 1 : retryCount;
    }

    public void setSaveTag(String saveTag) {
        this.saveTag = saveTag == null ? "" : saveTag;
    }

    public OperationBase clone() throws CloneNotSupportedException {
        OperationBase operationBase = (OperationBase) super.clone();
        operationBase.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration);
        operationBase.batchOperations = new BucketManager.BatchOperations();
        operationBase.errorLineList = new ArrayList<>();
        operationBase.fileMap = new FileMap(savePath, processName, saveTag + String.valueOf(++saveIndex));
        try {
            operationBase.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return operationBase;
    }

    public void processLine(List<Map<String, String>> lineList, int retryCount) throws IOException {
        int retry;
        for (Map<String, String> line : lineList) {
            retry = retryCount;
            while (retry > 0) {
                try {
                    bucketManager.prefetch(bucket, line.get("key"));
                    retry = 0;
                } catch (QiniuException e) {
                    retry--;
                    HttpResponseUtils.processException(e, retry, fileMap, new ArrayList<String>(){{ add(line.get("key")); }});
                }
            }
        }
    }

    public void processLine(List<Map<String, String>> fileInfoList) throws IOException {
        processLine(fileInfoList, retryCount);
    }

    public void closeResource() {
        fileMap.closeWriters();
    }
}
