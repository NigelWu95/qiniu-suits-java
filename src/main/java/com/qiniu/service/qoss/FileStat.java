package com.qiniu.service.qoss;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.persistence.FileMap;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.JsonConvertUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FileStat implements ILineProcess<Map<String, String>>, Cloneable {

    final private Auth auth;
    final private Configuration configuration;
    private BucketManager bucketManager;
    final private String bucket;
    final private String processName;
    private int retryCount;
    private boolean batch = true;
    private volatile BatchOperations batchOperations;
    private volatile List<String> errorLineList;
    final private String resultPath;
    private String resultTag;
    private int resultIndex;
    private FileMap fileMap;

    public FileStat(Auth auth, Configuration configuration, String bucket, String resultPath, int resultIndex)
            throws IOException {
        this.processName = "stat";
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

    public FileStat(Auth auth, Configuration configuration, String bucket, String resultPath) throws IOException {
        this(auth, configuration, bucket, resultPath, 0);
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

    public FileStat clone() throws CloneNotSupportedException {
        FileStat fileStat = (FileStat)super.clone();
        fileStat.bucketManager = new BucketManager(auth, configuration);
        fileStat.batchOperations = new BatchOperations();
        fileStat.fileMap = new FileMap(resultPath, processName, resultTag + String.valueOf(++resultIndex));
        try {
            fileStat.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return fileStat;
    }

    public String processLine(Map<String, String> line) throws QiniuException {
        FileInfo result = bucketManager.stat(bucket, line.get("key"));
        return JsonConvertUtils.toJsonWithoutUrlEscape(result);
    }

    synchronized private BatchOperations getOperations(List<Map<String, String>> lineList) {
        lineList.forEach(line -> {
            if (StringUtils.isNullOrEmpty(line.get("key")))
                errorLineList.add(String.valueOf(line) + "\tno target key in the line map.");
            else
                batchOperations.addStatOps(bucket, line.get("key"));
        });
        return batchOperations;
    }

    public void singleRun(List<Map<String, String>> fileInfoList, int retryCount) throws IOException {
        String result = null;
        for (Map<String, String> line : fileInfoList) {
            try {
                try {
                    result = processLine(line);
                } catch (QiniuException e) {
                    HttpResponseUtils.checkRetryCount(e, retryCount);
                    while (retryCount > 0) {
                        try {
                            result = processLine(line);
                            retryCount = 0;
                        } catch (QiniuException e1) {
                            retryCount = HttpResponseUtils.getNextRetryCount(e1, retryCount);
                        }
                    }
                }
                if (result != null && !"".equals(result)) fileMap.writeSuccess(result);
                else fileMap.writeError(line.get("key") + "\tempty stat result");
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileMap, new ArrayList<String>(){{add(line.get("key"));}});
            }
        }
    }

    public void batchRun(List<Map<String, String>> fileInfoList, int retryCount) throws IOException {
        int times = fileInfoList.size()/1000 + 1;
        List<Map<String, String>> processList;
        Response response = null;
        String result;
        JsonArray jsonArray;
        JsonObject jsonObject;
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
                    jsonArray = new Gson().fromJson(result, JsonArray.class);
                    for (int j = 0; j < processList.size(); j++) {
                        if (j < jsonArray.size()) {
                            jsonObject = jsonArray.get(j).getAsJsonObject();
                            if (jsonObject.get("code").getAsInt() == 200)
                                fileMap.writeSuccess(jsonObject.get("data").toString());
                            else
                                fileMap.writeError(processList.get(i).get("key") + "\t" + jsonObject.toString());
                        } else {
                            fileMap.writeError(processList.get(i).get("key") + "\tempty stat result");
                        }
                    }
                } catch (QiniuException e) {
                    HttpResponseUtils.processException(e, fileMap, processList.stream().map(line -> line.get("key"))
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
