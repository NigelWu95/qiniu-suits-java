package com.qiniu.process.qoss;

import com.google.gson.*;
import com.qiniu.common.QiniuException;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.JsonConvertUtils;
import com.qiniu.util.LineUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class StatFile extends Base {

    private BucketManager bucketManager;
    private BatchOperations batchOperations;
    private String format;
    private String separator;

    public StatFile(String accessKey, String secretKey, Configuration configuration, String bucket, String rmPrefix,
                    String savePath, String format, String separator, int saveIndex) throws IOException {
        super("stat", accessKey, secretKey, configuration, bucket, rmPrefix, savePath, saveIndex);
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        this.batchOperations = new BatchOperations();
        set(format, separator);
        this.batchSize = 1000;
    }

    public void updateStat(String bucket, String format, String separator, String rmPrefix) throws IOException {
        this.bucket = bucket;
        set(format, separator);
        this.rmPrefix = rmPrefix;
    }

    private void set(String format, String separator) throws IOException {
        this.format = format;
        if ("csv".equals(format) || "tab".equals(format)) {
            this.separator = "csv".equals(format) ? "," : separator;
        } else if (!"json".equals(this.format)) {
            throw new IOException("please check your format for line to map.");
        }
    }

    public StatFile(String accessKey, String secretKey, Configuration configuration, String bucket, String rmPrefix,
                    String savePath, String format, String separator) throws IOException {
        this(accessKey, secretKey, configuration, bucket, rmPrefix, savePath, format, separator, 0);
    }

    public StatFile clone() throws CloneNotSupportedException {
        StatFile statFile = (StatFile)super.clone();
        statFile.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        if (batchSize > 1) statFile.batchOperations = new BatchOperations();
        return statFile;
    }

    @Override
    synchronized protected String batchResult(List<Map<String, String>> lineList) throws QiniuException {
        batchOperations.clearOps();
        lineList.forEach(line -> batchOperations.addStatOps(bucket, line.get("key")));
        return HttpResponseUtils.getResult(bucketManager.batch(batchOperations));
    }

    @Override
    public void parseBatchResult(List<Map<String, String>> processList, String result) throws IOException {
        if (result == null || "".equals(result)) throw new IOException("not valid json.");
        JsonArray jsonArray;
        try {
            jsonArray = new Gson().fromJson(result, JsonArray.class);
        } catch (JsonParseException e) {
            throw new IOException("parse to json array error.");
        }
        JsonObject jsonObject;
        JsonObject data;
        for (int j = 0; j < processList.size(); j++) {
            if (j < jsonArray.size()) {
                jsonObject = jsonArray.get(j).getAsJsonObject();
                if (!(jsonObject.get("data") instanceof JsonNull) && jsonObject.get("data") instanceof JsonObject) {
                    data = jsonObject.get("data").getAsJsonObject();
                } else {
                    fileMap.writeError(processList.get(j).get("key") + "\t" + jsonObject.toString(), false);
                    continue;
                }
                if (jsonObject.get("code").getAsInt() == 200) {
                    // stat 接口查询结果不包含文件名，故再加入对应的文件名
                    data.addProperty("key", processList.get(j).get("key"));
                    if (!"json".equals(format)) {
                        fileMap.writeSuccess(LineUtils.toFormatString(data, separator, null), false);
                    } else {
                        fileMap.writeSuccess(data.toString(), false);
                    }
                } else {
                    fileMap.writeError(processList.get(j).get("key") + "\t" + jsonObject.toString(), false);
                }
            } else {
                fileMap.writeError(processList.get(j).get("key") + "\tempty stat result", false);
            }
        }
    }

    @Override
    protected String singleResult(Map<String, String> line) throws QiniuException {
        FileInfo fileInfo = bucketManager.stat(bucket, line.get("key"));
        fileInfo.key = line.get("key");
        if (!"json".equals(format)) {
            try {
                return LineUtils.toFormatString(fileInfo, separator, null);
            } catch (IOException e) {
                throw new QiniuException(e);
            }
        } else {
            return JsonConvertUtils.toJsonWithoutUrlEscape(fileInfo);
        }
    }
}
