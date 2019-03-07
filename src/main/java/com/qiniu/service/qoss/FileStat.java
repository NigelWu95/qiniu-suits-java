package com.qiniu.service.qoss;

import com.google.gson.*;
import com.qiniu.common.QiniuException;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.service.line.JsonObjParser;
import com.qiniu.service.line.MapToTableFormatter;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileStat extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    private String format;
    private JsonObjParser jsonObjParser;
    private IStringFormat<Map<String, String>> stringFormatter;

    public FileStat(String accessKey, String secretKey, Configuration configuration, String bucket, String savePath,
                    String format, int saveIndex) throws IOException {
        super("stat", accessKey, secretKey, configuration, bucket, savePath, saveIndex);
        this.format = format;
        if ("table".equals(format)) {
            Map<String, String> indexMap = new HashMap<String, String>(){{
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
        }
        this.stringFormatter = new MapToTableFormatter("\t", null);
    }

    public FileStat(String accessKey, String secretKey, Configuration configuration, String bucket, String savePath,
                    String format) throws IOException {
        this(accessKey, secretKey, configuration, bucket, savePath, format, 0);
    }

    public FileStat clone() throws CloneNotSupportedException {
        FileStat fileStat = (FileStat)super.clone();
        fileStat.stringFormatter = new MapToTableFormatter("\t", null);
        return fileStat;
    }

    public String getInputParams(Map<String, String> line) {
        return line.get("key");
    }

    synchronized public BatchOperations getOperations(List<Map<String, String>> lineList) {
        batchOperations.clearOps();
        lineList.forEach(line -> {
            if (line.get("key") == null)
                errorLineList.add(String.valueOf(line) + "\tno target key in the line map.");
            else
                batchOperations.addStatOps(bucket, line.get("key"));
        });
        return batchOperations;
    }

    @Override
    public void parseBatchResult(List<Map<String, String>> processList, String result) throws IOException {
        if (result == null || "".equals(result)) throw new QiniuException(null, "not valid json.");
        JsonArray jsonArray;
        try {
            jsonArray = new Gson().fromJson(result, JsonArray.class);
        } catch (JsonParseException e) { throw new QiniuException(null, "parse to json array error.");}
        for (int j = 0; j < processList.size(); j++) {
            if (j < jsonArray.size()) {
                JsonObject jsonObject = jsonArray.get(j).getAsJsonObject();
                jsonObject.get("data").getAsJsonObject()
                        .addProperty("key", processList.get(j).get("key"));
                if (jsonObject.get("code").getAsInt() == 200)
                    if ("table".equals(format))
                        fileMap.writeSuccess( stringFormatter.toFormatString(
                                jsonObjParser.getItemMap(jsonObject.get("data").getAsJsonObject())), false);
                    else fileMap.writeSuccess(jsonObject.get("data").toString(), false);
                else
                    fileMap.writeError(processList.get(j).get("key") + "\t" + jsonObject.toString(), false);
            } else {
                fileMap.writeError(processList.get(j).get("key") + "\tempty stat result", false);
            }
        }
    }
}
