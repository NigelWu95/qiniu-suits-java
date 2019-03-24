package com.qiniu.process.qoss;

import com.google.gson.*;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.interfaces.IStringFormat;
import com.qiniu.line.JsonObjParser;
import com.qiniu.line.MapToTableFormatter;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileStat extends Base {

    private String format;
    private String separator;
    private JsonObjParser jsonObjParser;
    private IStringFormat<Map<String, String>> stringFormatter;
    private BucketManager bucketManager;

    public FileStat(String accessKey, String secretKey, Configuration configuration, String bucket, String rmPrefix,
                    String savePath, String format, String separator, int saveIndex) throws IOException {
        super("stat", accessKey, secretKey, configuration, bucket, rmPrefix, savePath, saveIndex);
        this.format = format;
        if ("csv".equals(format) || "tab".equals(format)) {
            this.separator = "csv".equals(format) ? "," : separator;
            HashMap<String, String> indexMap = new HashMap<String, String>(){{
                put("key", "key");
                put("hash", "hash");
                put("fsize", "fsize");
                put("putTime", "putTime");
                put("mimeType", "mimeType");
                put("type", "type");
                put("status", "status");
                put("endUser", "endUser");
                put("md5", "md5");
            }};
            this.jsonObjParser = new JsonObjParser(indexMap, true);
        } else if (!"json".equals(this.format)) {
            throw new IOException("please check your format for line to map.");
        }
        this.stringFormatter = new MapToTableFormatter(this.separator, null);
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration);
    }

    public FileStat(String accessKey, String secretKey, Configuration configuration, String bucket, String rmPrefix,
                    String savePath, String format, String separator) throws IOException {
        this(accessKey, secretKey, configuration, bucket, rmPrefix, savePath, format, separator, 0);
    }

    public FileStat clone() throws CloneNotSupportedException {
        FileStat fileStat = (FileStat)super.clone();
        fileStat.jsonObjParser = jsonObjParser.clone();
        fileStat.stringFormatter = new MapToTableFormatter(separator, null);
        fileStat.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration);
        return fileStat;
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
                        fileMap.writeSuccess(stringFormatter.toFormatString(jsonObjParser.getItemMap(data)), false);
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

    protected Response batchResult(List<Map<String, String>> lineList) throws QiniuException {
        BucketManager.BatchOperations batchOperations = new BucketManager.BatchOperations();
        lineList.forEach(line -> batchOperations.addStatOps(bucket, line.get("key")));
        return bucketManager.batch(batchOperations);
    }

    protected String singleResult(Map<String, String> line) throws QiniuException {
        return null;
    }
}
