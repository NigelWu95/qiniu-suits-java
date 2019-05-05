package com.qiniu.process.qoss;

import com.google.gson.*;
import com.qiniu.common.QiniuException;
import com.qiniu.convert.JsonToString;
import com.qiniu.convert.QOSObjToString;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StatFile extends Base<Map<String, String>> {

    private String format;
    private String separator;
    private ITypeConvert typeConverter;
    private BatchOperations batchOperations;
    private BucketManager bucketManager;

    public StatFile(String accessKey, String secretKey, Configuration configuration, String bucket, String savePath,
                    String format, String separator, int saveIndex) throws IOException {
        super("stat", accessKey, secretKey, configuration, bucket, savePath, saveIndex);
        set(format, separator);
        this.batchSize = 1000;
        this.batchOperations = new BatchOperations();
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
    }

    public void updateStat(String bucket, String format, String separator) throws IOException {
        this.bucket = bucket;
        set(format, separator);
    }

    private void set(String format, String separator) throws IOException {
        this.format = format;
        if ("csv".equals(format) || "tab".equals(format)) {
            this.separator = "csv".equals(format) ? "," : separator;
        } else if (!"json".equals(this.format)) {
            throw new IOException("please check your format for line to map.");
        }
        if (batchSize > 1) typeConverter = new JsonToString(format, separator, null);
        else typeConverter = new QOSObjToString(format, separator, null);
    }

    public StatFile(String accessKey, String secretKey, Configuration configuration, String bucket, String savePath,
                    String format, String separator) throws IOException {
        this(accessKey, secretKey, configuration, bucket, savePath, format, separator, 0);
    }

    public StatFile clone() throws CloneNotSupportedException {
        StatFile statFile = (StatFile)super.clone();
        statFile.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        if (batchSize > 1) {
            statFile.batchOperations = new BatchOperations();
            try {
                statFile.typeConverter = new JsonToString(format, separator, null);
            } catch (IOException e) {
                throw new CloneNotSupportedException(e.getMessage() + ", init writer failed.");
            }
        } else {
            try {
                statFile.typeConverter = new QOSObjToString(format, separator, null);
            } catch (IOException e) {
                throw new CloneNotSupportedException(e.getMessage() + ", init writer failed.");
            }
        }
        return statFile;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get("key");
    }

    @Override
    protected boolean validCheck(Map<String, String> line) {
        return line.get("key") == null;
    }

    @Override
    synchronized protected String batchResult(List<Map<String, String>> lineList) throws QiniuException {
        batchOperations.clearOps();
        lineList.forEach(line -> batchOperations.addStatOps(bucket, line.get("key")));
        return HttpResponseUtils.getResult(bucketManager.batch(batchOperations));
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Map<String, String>> parseBatchResult(List<Map<String, String>> processList, String result)
            throws IOException {
        if (result == null || "".equals(result)) throw new IOException("not valid json.");
        List<Map<String, String>> retryList = new ArrayList<>();
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
                    fileSaveMapper.writeError(processList.get(j).get("key") + "\t" + jsonObject.toString(), false);
                    continue;
                }
                switch (HttpResponseUtils.checkStatusCode(jsonObject.get("code").getAsInt())) {
                    case 1:
                        data.addProperty("key", processList.get(j).get("key"));
                        fileSaveMapper.writeSuccess((String) typeConverter.convertToV(data), false);
                        break;
                    case 0:
                        retryList.add(processList.get(j)); // 放回重试列表
                        break;
                    case -1:
                        fileSaveMapper.writeError(processList.get(j).get("key") + "\t" + jsonObject.toString(), false);
                        break;
                }
            } else {
                fileSaveMapper.writeError(processList.get(j).get("key") + "\tempty stat result", false);
            }
        }
        return retryList;
    }

    @Override
    protected void parseSingleResult(Map<String, String> line, String result) throws IOException {
        fileSaveMapper.writeSuccess(result, false);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected String singleResult(Map<String, String> line) throws QiniuException {
        FileInfo fileInfo = bucketManager.stat(bucket, line.get("key"));
        fileInfo.key = line.get("key");
        try {
            return (String) typeConverter.convertToV(fileInfo);
        } catch (IOException e) {
            throw new QiniuException(e, e.getMessage());
        }
    }
}
