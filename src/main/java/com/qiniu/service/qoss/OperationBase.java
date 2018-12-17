package com.qiniu.service.qoss;

import com.qiniu.persistence.FileMap;
import com.qiniu.sdk.BucketManager;
import com.qiniu.sdk.BucketManager.*;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    protected Auth auth;
    protected Configuration configuration;
    protected BucketManager bucketManager;
    protected String bucket;
    protected String processName;
    protected boolean batch;
    protected volatile BatchOperations batchOperations;
    protected int retryCount;
    protected String resultFileDir;
    protected FileMap fileMap;

    public OperationBase(Auth auth, Configuration configuration, String bucket, String resultFileDir) {
        this.auth = auth;
        this.configuration = configuration;
        this.bucketManager = new BucketManager(auth, configuration);
        this.bucket = bucket;
        this.batchOperations = new BatchOperations();
        this.resultFileDir = resultFileDir;
        this.fileMap = new FileMap();
    }

    public OperationBase clone() throws CloneNotSupportedException {
        OperationBase operationBase = (OperationBase)super.clone();
        operationBase.bucketManager = new BucketManager(auth, configuration);
        operationBase.batchOperations = new BatchOperations();
        operationBase.fileMap = new FileMap();
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

    protected abstract Response getResponse(Map<String, String> fileInfo) throws QiniuException;

    protected abstract BatchOperations getOperations(List<Map<String, String>> fileInfoList);

    public List<String> singleRun(List<Map<String, String>> fileInfoList) throws QiniuException {

        List<String> resultList = new ArrayList<>();
        for (Map<String, String> fileInfo : fileInfoList) {
            try {
                Response response = null;
                try {
                    response = getResponse(fileInfo);
                } catch (QiniuException e) {
                    HttpResponseUtils.checkRetryCount(e, retryCount);
                    while (retryCount > 0) {
                        try {
                            response = getResponse(fileInfo);
                            retryCount = 0;
                        } catch (QiniuException e1) {
                            retryCount = HttpResponseUtils.getNextRetryCount(e1, retryCount);
                        }
                    }
                }
                String result = HttpResponseUtils.getResult(response);
                if (!StringUtils.isNullOrEmpty(result)) resultList.add(result);
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileMap, processName, fileInfo.get("key"));
            }
        }

        return resultList;
    }

    public List<String> batchRun(List<Map<String, String>> fileInfoList) throws QiniuException {

        List<String> resultList = new ArrayList<>();
        int times = fileInfoList.size()/1000 + 1;
        for (int i = 0; i < times; i++) {
            List<Map<String, String>> processList = fileInfoList.subList(1000 * i, i == times - 1 ?
                    fileInfoList.size() : 1000 * (i + 1));
            if (processList.size() > 0) {
                try {
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
                    String result = HttpResponseUtils.getResult(response);
                    if (!StringUtils.isNullOrEmpty(result)) resultList.add(result);
                } catch (QiniuException e) {
                    HttpResponseUtils.processException(e, fileMap, processName, String.join("\n", processList.stream()
                                    .map(fileInfo -> fileInfo.get("key")).collect(Collectors.toList())));
                }
            }
        }
        return resultList;
    }

    public void processLine(List<Map<String, String>> fileInfoList) throws QiniuException {

        List<String> resultList = batch ? batchRun(fileInfoList) : singleRun(fileInfoList);
        if (resultList.size() > 0) fileMap.writeSuccess(String.join("\n", resultList));
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}
