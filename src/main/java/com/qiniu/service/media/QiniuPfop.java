package com.qiniu.service.media;

import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.sdk.OperationManager;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.StringMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QiniuPfop implements ILineProcess<Map<String, String>>, Cloneable {

    public Auth auth;
    public Configuration configuration;
    public OperationManager operationManager;
    public String bucket;
    public String pipeline;
    public String processName;
    public boolean batch;
    public int retryCount;
    public String resultFileDir;
    public FileMap fileMap;

    private void initBaseParams() {
        this.processName = "pfop";
    }

    public QiniuPfop(Auth auth, Configuration configuration, String bucket, String pipeline, String resultFileDir) {
        this.auth = auth;
        this.configuration = configuration;
        this.operationManager = new OperationManager(auth, configuration);
        this.bucket = bucket;
        this.pipeline = pipeline;
        this.resultFileDir = resultFileDir;
        initBaseParams();
        this.fileMap = new FileMap();
    }

    public QiniuPfop(Auth auth, Configuration configuration, String bucket, String pipeline, String resultFileDir,
                     int resultFileIndex) throws IOException {
        this(auth, configuration, bucket, pipeline, resultFileDir);
        this.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public QiniuPfop getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        QiniuPfop qiniuPfop = (QiniuPfop)super.clone();
        qiniuPfop.operationManager = new OperationManager(auth, configuration);
        qiniuPfop.fileMap = new FileMap();
        try {
            qiniuPfop.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return qiniuPfop;
    }

    public void setBatch(boolean batch) {
        this.batch = batch;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public String getProcessName() {
        return this.processName;
    }

    public String singleWithRetry(Map<String, String> line, int retryCount) throws QiniuException {

        String persistentId = null;
        try {
            persistentId = operationManager.pfop(bucket, line.get("key"), line.get("fops"),
                    new StringMap().putNotEmpty("pipeline", pipeline));
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    persistentId = operationManager.pfop(bucket, line.get("key"), line.get("fops"),
                            new StringMap().putNotEmpty("pipeline", pipeline));
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        } catch (Exception e) {
            throw new QiniuException(e, e.getMessage());
        }

        return persistentId;
    }

    public void processLine(List<Map<String, String>> lineList) throws QiniuException {

        List<String> resultList = new ArrayList<>();
        for (Map<String, String> line : lineList) {
            try {
                String result = singleWithRetry(line, retryCount);
                if (result != null && !"".equals(result)) resultList.add(result);
                else throw new QiniuException(null, "empty pfop persistent id");
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileMap, processName, line.get("key") + "\t" +
                        line.get("fops"));
            }
        }
        if (resultList.size() > 0) fileMap.writeSuccess(String.join("\n", resultList));
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}