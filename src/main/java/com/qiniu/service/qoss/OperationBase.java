package com.qiniu.service.qoss;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
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

    public OperationBase(String processName, Auth auth, Configuration configuration, String bucket, String resultPath,
                         int resultIndex) throws IOException {
        this.processName = processName;
        this.auth = auth;
        this.configuration = configuration;
        this.bucketManager = new BucketManager(auth, configuration);
        this.bucket = bucket;
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
        this.retryCount = retryCount < 1 ? 1 : retryCount;
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
        operationBase.errorLineList = new ArrayList<>();
        operationBase.fileMap = new FileMap(resultPath, processName, resultTag + String.valueOf(++resultIndex));
        try {
            operationBase.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return operationBase;
    }

    protected abstract String processLine(Map<String, String> fileInfo) throws QiniuException;

    protected abstract BatchOperations getOperations(List<Map<String, String>> fileInfoList);

    // 获取输入行中的关键参数，将其保存到对应结果的行当中，方便确定对应关系和失败重试
    protected abstract String getInputParams(Map<String, String> line);

    public void singleRun(List<Map<String, String>> fileInfoList, int retryCount) throws QiniuException {
        String result = null;
        for (Map<String, String> line : fileInfoList) {
            int retry = retryCount;
            while (retry > 0) {
                try {
                    result = processLine(line);
                    retry = 0;
                } catch (QiniuException e) {
                    retryCount--;
                    HttpResponseUtils.processException(e, retry, fileMap,
                            new ArrayList<String>(){{add(getInputParams(line));}});
                }
            }
            if (result != null && !"".equals(result)) fileMap.writeSuccess(getInputParams(line) + "\t" + result);
            else fileMap.writeError(getInputParams(line) + "\tempty result");
        }
    }

    public void parseBatchResult(List<Map<String, String>> processList, String result) throws QiniuException {
        if (result == null || "".equals(result)) throw new QiniuException(null, "not valid json.");
        JsonArray jsonArray;
        try {
            jsonArray = new Gson().fromJson(result, JsonArray.class);
        } catch (JsonParseException e) { throw new QiniuException(null, "parse to json array error.");}
        for (int j = 0; j < processList.size(); j++) {
            JsonObject jsonObject = jsonArray.get(j).getAsJsonObject();
            if (j < jsonArray.size()) {
                if (jsonObject.get("code").getAsInt() == 200)
                    fileMap.writeSuccess(getInputParams(processList.get(j)) + "\t" + jsonObject);
                else
                    fileMap.writeError(getInputParams(processList.get(j)) + "\t" + jsonObject);
            } else {
                fileMap.writeError(getInputParams(processList.get(j)) + "\tempty result");
            }
        }
    }

    public void batchRun(List<Map<String, String>> fileInfoList, int retryCount) throws QiniuException {
        int times = fileInfoList.size()/1000 + 1;
        List<Map<String, String>> processList;
        Response response;
        String result;
        for (int i = 0; i < times; i++) {
            processList = fileInfoList.subList(1000 * i, i == times - 1 ? fileInfoList.size() : 1000 * (i + 1));
            if (processList.size() > 0) {
                batchOperations = getOperations(processList);
                int count = retryCount;
                while (count > 0) {
                    try {
                        response = bucketManager.batch(batchOperations);
                        result = HttpResponseUtils.getResult(response);
                        parseBatchResult(processList, result);
                        count = 0;
                    } catch (QiniuException e) {
                        retryCount--;
                        HttpResponseUtils.processException(e, retryCount, fileMap,
                                processList.stream().map(this::getInputParams).collect(Collectors.toList()));
                    }
                }
                batchOperations.clearOps();
            }
        }
    }

    public void processLine(List<Map<String, String>> fileInfoList) throws QiniuException {
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
