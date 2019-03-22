package com.qiniu.process;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.persistence.FileMap;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.BatchOperations;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.FileNameUtils;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.LogUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class Base implements ILineProcess<Map<String, String>>, Cloneable {

    final protected String processName;
    protected int retryTimes = 3;
    final protected Configuration configuration;
    protected BucketManager bucketManager;
    protected volatile BatchOperations batchOperations;
    protected volatile List<String> errorLineList;
    final protected String accessKey;
    final protected String secretKey;
    final protected String bucket;
    final private String rmPrefix;
    protected String saveTag;
    protected int saveIndex;
    final protected String savePath;
    protected FileMap fileMap;

    public Base(String processName, String accessKey, String secretKey, Configuration configuration,
                String bucket, String rmPrefix, String savePath, int saveIndex) throws IOException {
        this.processName = processName;
        this.configuration = configuration;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration);
        this.batchOperations = new BatchOperations();
        this.errorLineList = new ArrayList<>();
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.bucket = bucket;
        this.rmPrefix = rmPrefix;
        this.saveTag = "";
        this.saveIndex = saveIndex;
        this.savePath = savePath;
        this.fileMap = new FileMap(savePath, processName, String.valueOf(saveIndex));
        this.fileMap.initDefaultWriters();
    }

    public String getProcessName() {
        return this.processName;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes < 1 ? 3 : retryTimes;
    }

    public void setSaveTag(String saveTag) {
        this.saveTag = saveTag == null ? "" : saveTag;
    }

    public Base clone() throws CloneNotSupportedException {
        Base base = (Base)super.clone();
        base.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        base.batchOperations = new BatchOperations();
        base.errorLineList = new ArrayList<>();
        base.fileMap = new FileMap(savePath, processName, saveTag + String.valueOf(++saveIndex));
        try {
            base.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return base;
    }

    /**
     * 实现从 fileInfoList 转换得到 batch 操作的指令集 batchOperations，需要先清除 batchOperations 中可能存在的上次的内容
     * @param lineList 输入的行信息列表，应当是校验之后的列表（不包含空行或者确实 key 字段的行）
     * @return 输入 lineList 转换之后的 batchOperations
     */
    protected abstract BatchOperations getBatchOperations(List<Map<String, String>> lineList);

    /**
     * 处理 batchOperations 执行的结果，将输入的文件信息和结果对应地记录下来
     * @param processList batch 操作的资源列表
     * @param result batch 操作之后的响应结果
     * @throws IOException 写入结果失败抛出的异常
     */
    public void parseBatchResult(List<Map<String, String>> processList, String result) throws IOException {
        if (result == null || "".equals(result)) throw new IOException("not valid json.");
        JsonArray jsonArray;
        try {
            jsonArray = new Gson().fromJson(result, JsonArray.class);
        } catch (JsonParseException e) {
            throw new IOException("parse to json array error.");
        }
        JsonObject jsonObject;
        for (int j = 0; j < processList.size(); j++) {
            jsonObject = jsonArray.get(j).getAsJsonObject();
            // 正常情况下 jsonArray 和 processList 的长度是相同的，将输入行信息和执行结果一一对应记录，否则结果记录为空
            if (j < jsonArray.size()) {
                if (jsonObject.get("code").getAsInt() == 200)
                    fileMap.writeSuccess(String.valueOf(processList.get(j)) + "\t" + jsonObject, false);
                else
                    fileMap.writeError(String.valueOf(processList.get(j)) + "\t" + jsonObject, false);
            } else {
                fileMap.writeKeyFile("empty_result", String.valueOf(processList.get(j)), false);
            }
        }
    }

    /**
     * 批量处理输入行，具体执行的操作取决于 batchOperations 设置的指令（通过子类去设置）
     * @param lineList 输入列表
     * @param retryTimes 每一行信息处理时需要的重试次数
     * @throws IOException 处理失败可能抛出的异常
     */
    public void batchProcess(List<Map<String, String>> lineList, int retryTimes) throws IOException {
        // 先进行过滤修改
        lineList = lineList.parallelStream().filter(line -> {
            try {
                line.put("key", FileNameUtils.rmPrefix(rmPrefix, line.get("key")));
                return true;
            } catch (IOException e) {
                errorLineList.add(String.valueOf(line) + "\t" + e.getMessage());
                return false;
            }
        }).collect(Collectors.toList());
        int times = lineList.size()/1000 + 1;
        List<Map<String, String>> processList;
        Response response;
        String result;
        int retry;
        for (int i = 0; i < times; i++) {
            processList = lineList.subList(1000 * i, i == times - 1 ? lineList.size() : 1000 * (i + 1));
            if (processList.size() > 0) {
                batchOperations = getBatchOperations(processList);
                retry = retryTimes;
                while (retry > 0) {
                    try {
                        response = bucketManager.batch(batchOperations);
                        result = HttpResponseUtils.getResult(response);
                        parseBatchResult(processList, result);
                        retry = 0;
                    } catch (QiniuException e) {
                        retry = HttpResponseUtils.checkException(e, retry);
                        if (retry < 0) LogUtils.writeLog(e, fileMap, lineList.subList(i, lineList.size() - 1)
                                .parallelStream().map(String::valueOf).collect(Collectors.toList()));
                        if (retry == -1) throw e;
                    }
                }
            }
        }
        if (errorLineList.size() > 0) {
            fileMap.writeError(String.join("\n", errorLineList), false);
            errorLineList.clear();
        }
    }

    public void processLine(List<Map<String, String>> lineList) throws IOException {
        batchProcess(lineList, retryTimes);
    }

    public void closeResource() {
        fileMap.closeWriters();
    }
}
