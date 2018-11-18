package com.qiniu.service.oss;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.sdk.BucketManager;
import com.qiniu.sdk.BucketManager.*;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class OperationBase {

    protected Auth auth;
    protected Configuration configuration;
    protected BucketManager bucketManager;
    protected String bucket;
    protected String processName;
    protected boolean batch = true;
    protected volatile BatchOperations batchOperations;
    protected String resultFileDir;
    protected FileReaderAndWriterMap fileReaderAndWriterMap;
    protected int retryCount = 3;

    public OperationBase(Auth auth, Configuration configuration, String bucket, String resultFileDir) {
        this.auth = auth;
        this.configuration = configuration;
        this.bucketManager = new BucketManager(auth, configuration);
        this.bucket = bucket;
        this.batchOperations = new BatchOperations();
        this.resultFileDir = resultFileDir;
        this.fileReaderAndWriterMap = new FileReaderAndWriterMap();
    }

    public OperationBase clone() throws CloneNotSupportedException {
        OperationBase operationBase = (OperationBase)super.clone();
        operationBase.bucketManager = new BucketManager(auth, configuration);
        operationBase.batchOperations = new BatchOperations();
        operationBase.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        return operationBase;
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

    protected abstract Response getResponse(FileInfo fileInfo) throws QiniuException;

    public String getResult(Response response) throws QiniuException {
        if (response == null) return null;
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        String reqId = response.reqId;
        response.close();
        return statusCode + "\t" + reqId + "\t" + responseBody;
    }

    public Response singleWithRetry(FileInfo fileInfo, int retryCount) throws QiniuException {

        Response response = null;
        try {
            response = getResponse(fileInfo);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    response = getResponse(fileInfo);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }

    public String run(FileInfo fileInfo, int retryCount) throws QiniuException {
        Response response = singleWithRetry(fileInfo, retryCount);
        return getResult(response);
    }

    synchronized public Response batchWithRetry(List<FileInfo> fileInfoList, int retryCount) throws QiniuException {

        Response response = null;
        batchOperations = getOperations(fileInfoList);
        try {
            response = bucketManager.batch(batchOperations);
        } catch (QiniuException e) {
            HttpResponseUtils.checkRetryCount(e, retryCount);
            while (retryCount > 0) {
                try {
                    response = bucketManager.batch(batchOperations);
                    retryCount = 0;
                } catch (QiniuException e1) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e1, retryCount);
                }
            }
        }
        batchOperations.clearOps();

        return response;
    }

    protected abstract BatchOperations getOperations(List<FileInfo> fileInfoList);

    public String batchRun(List<FileInfo> fileInfoList, int retryCount) throws QiniuException {
        Response response = batchWithRetry(fileInfoList, retryCount);
        return getResult(response);
    }

    protected abstract String getInfo();

    public void processFile(List<FileInfo> fileInfoList, int retryCount) throws QiniuException {

        fileInfoList = fileInfoList == null ? null : fileInfoList.parallelStream()
                .filter(Objects::nonNull).collect(Collectors.toList());
        if (fileInfoList == null || fileInfoList.size() == 0) return;

        if (batch) {
            int times = fileInfoList.size()/1000 + 1;
            for (int i = 0; i < times; i++) {
                List<FileInfo> processList = fileInfoList.subList(1000 * i, i == times - 1 ?
                        fileInfoList.size() : 1000 * (i + 1));
                if (processList.size() > 0) {
                    try {
                        String result = batchRun(processList, retryCount);
                        if (!StringUtils.isNullOrEmpty(result)) fileReaderAndWriterMap.writeSuccess(result);
                    } catch (QiniuException e) {
                        HttpResponseUtils.processException(e, fileReaderAndWriterMap, processName, getInfo() + "\t" +
                                String.join(",", processList.stream()
                                        .map(fileInfo -> fileInfo.key).collect(Collectors.toList())));
                    }
                }
            }
            return;
        }

        List<String> resultList = new ArrayList<>();
        for (FileInfo fileInfo : fileInfoList) {
            try {
                String result = run(fileInfo, retryCount);
                if (!StringUtils.isNullOrEmpty(result)) resultList.add(result);
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileReaderAndWriterMap, processName, getInfo() +
                        "\t" + fileInfo.key);
            }
        }
        if (resultList.size() > 0) fileReaderAndWriterMap.writeSuccess(String.join("\n", resultList));
    }

    public void closeResource() {
        fileReaderAndWriterMap.closeWriter();
    }
}
