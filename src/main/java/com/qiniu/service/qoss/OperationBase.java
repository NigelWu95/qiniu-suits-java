package com.qiniu.service.qoss;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.qiniu.persistence.FileMap;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    final protected Auth auth;
    final protected Configuration configuration;
    protected BucketManager bucketManager;
    final protected String bucket;
    final protected String processName;
    protected int retryCount;
    protected boolean batch = true;
    protected volatile BatchOperations batchOperations;
    protected volatile List<String> errorLineList;
    final protected String resultPath;
    protected String resultTag;
    protected int resultIndex;
    protected FileMap fileMap;

    public OperationBase(Auth auth, Configuration configuration, String bucket, String processName, String resultPath,
                         int resultIndex) throws IOException {
        this.auth = auth;
        this.configuration = configuration;
        this.bucketManager = new BucketManager(auth, configuration);
        this.bucket = bucket;
        this.processName = processName;
        this.batchOperations = new BatchOperations();
        this.errorLineList = new ArrayList<>();
        this.resultPath = resultPath;
        this.resultTag = "";
        this.resultIndex = resultIndex;
        this.fileMap = new FileMap(resultPath, processName, String.valueOf(resultIndex));
        this.fileMap.initDefaultWriters();
    }

    public String getProcessName() {
        return this.processName;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public void setBatch(boolean batch) {
        this.batch = batch;
    }

    public void setResultTag(String resultTag) {
        this.resultTag = resultTag == null ? "" : resultTag;
    }

    public OperationBase clone() throws CloneNotSupportedException {
        OperationBase operationBase = (OperationBase)super.clone();
        operationBase.bucketManager = new BucketManager(auth, configuration);
        operationBase.batchOperations = new BatchOperations();
        operationBase.fileMap = new FileMap(resultPath, processName, resultTag + String.valueOf(++resultIndex));
        try {
            operationBase.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return operationBase;
    }

    protected abstract String processLine(Map<String, String> fileInfo) throws IOException;

    protected abstract BatchOperations getOperations(List<Map<String, String>> fileInfoList);

    public void singleRun(List<Map<String, String>> fileInfoList, int retryCount) throws IOException {
        String key;
        String result = null;
        for (Map<String, String> fileInfo : fileInfoList) {
            key = fileInfo.get("key");
            try {
                try {
                    result = processLine(fileInfo);
                } catch (QiniuException e) {
                    HttpResponseUtils.checkRetryCount(e, retryCount);
                    while (retryCount > 0) {
                        try {
                            result = processLine(fileInfo);
                            retryCount = 0;
                        } catch (QiniuException e1) {
                            retryCount = HttpResponseUtils.getNextRetryCount(e1, retryCount);
                        }
                    }
                }
                if (result != null && !"".equals(result)) fileMap.writeSuccess(key + "\t" + result);
                else fileMap.writeError(key + "\t" +  String.valueOf(fileInfo) + "\tempty result");
            } catch (QiniuException e) {
                String finalKey = key;
                HttpResponseUtils.processException(e, fileMap, new ArrayList<String>(){{
                    add(finalKey + "\t" + String.valueOf(fileInfo));
                }});
            }
        }
    }

    public void batchRun(List<Map<String, String>> fileInfoList, int retryCount) throws IOException {
        int times = fileInfoList.size()/1000 + 1;
        List<Map<String, String>> processList;
        Response response = null;
        String result;
        for (int i = 0; i < times; i++) {
            processList = fileInfoList.subList(1000 * i, i == times - 1 ? fileInfoList.size() : 1000 * (i + 1));
            if (processList.size() > 0) {
                batchOperations = getOperations(processList);
                try {
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
                    result = HttpResponseUtils.getResult(response);
                    JsonArray jsonArray = new Gson().fromJson(result, JsonArray.class);
                    for (int j = 0; j < processList.size(); j++) {
                        if (j < jsonArray.size())
                            fileMap.writeSuccess(processList.get(j).get("key") + "\t" + jsonArray.get(j));
                        else
                            fileMap.writeError(processList.get(j).get("key") + "\tempty result");
                    }
                } catch (QiniuException e) {
                    HttpResponseUtils.processException(e, fileMap, processList.stream()
                            .map(line -> line.get("key") + "\t" + String.valueOf(line))
                            .collect(Collectors.toList()));
                }
            }
        }
    }

    public void processLine(List<Map<String, String>> fileInfoList) throws IOException {
        if (batch) batchRun(fileInfoList, retryCount);
        else singleRun(fileInfoList, retryCount);
        if (errorLineList.size() > 0) {
            fileMap.writeError(String.join("\n", errorLineList));
            errorLineList.clear();
        }
    }

    public void closeResource() {
        fileMap.closeWriters();
    }
}
