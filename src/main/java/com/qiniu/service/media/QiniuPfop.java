package com.qiniu.service.media;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.model.media.Avinfo;
import com.qiniu.sdk.OperationManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.StringMap;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class QiniuPfop implements Cloneable {

    public Auth auth;
    public Configuration configuration;
    public OperationManager operationManager;
    public String bucket;
    public String pipeline;
    public String processName;
    public boolean batch = true;
    public int retryCount = 3;
    public String resultFileDir;
    public FileReaderAndWriterMap fileReaderAndWriterMap;

    public QiniuPfop(Auth auth, Configuration configuration, String bucket, String pipeline, String resultFileDir) {
        this.auth = auth;
        this.configuration = configuration;
        this.operationManager = new OperationManager(auth, configuration);
        this.bucket = bucket;
        this.pipeline = pipeline;
        this.resultFileDir = resultFileDir;
        this.fileReaderAndWriterMap = new FileReaderAndWriterMap();
    }

    public QiniuPfop(Auth auth, Configuration configuration, String bucket, String pipeline, String resultFileDir,
                     int resultFileIndex) throws IOException {
        this(auth, configuration, bucket, pipeline, resultFileDir);
        this.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public QiniuPfop getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        QiniuPfop qiniuPfop = (QiniuPfop)super.clone();
        qiniuPfop.operationManager = new OperationManager(auth, configuration);
        qiniuPfop.fileReaderAndWriterMap = new FileReaderAndWriterMap();
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

    public String getInfo() {
        return bucket + "\t" + pipeline;
    }

    public String singleWithRetry(String fopInfo, int retryCount) throws QiniuException {

        String persistentId = null;
        String[] items = fopInfo.split("\t");
        try {
            persistentId = operationManager.pfop(bucket, items[0], items[1],
                    new StringMap().putNotEmpty("pipeline", pipeline));
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    persistentId = operationManager.pfop(bucket, items[0], items[1],
                            new StringMap().putNotEmpty("pipeline", pipeline));
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return persistentId;
    }

    public void processFile(List<String> fopInfoList, int retryCount) throws QiniuException {

        fopInfoList = fopInfoList == null ? null : fopInfoList.parallelStream()
                .filter(Objects::nonNull).collect(Collectors.toList());
        if (fopInfoList == null || fopInfoList.size() == 0) return;
        List<String> resultList = new ArrayList<>();
        for (String fopInfo : fopInfoList) {
            try {
                String result = singleWithRetry(fopInfo, retryCount);
                if (!StringUtils.isNullOrEmpty(result)) resultList.add(result);
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileReaderAndWriterMap, processName, getInfo() +
                        "\t" + fopInfo);
            }
        }
        if (resultList.size() > 0) fileReaderAndWriterMap.writeSuccess(String.join("\n", resultList));
    }
}