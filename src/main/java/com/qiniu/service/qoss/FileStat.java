package com.qiniu.service.qoss;

import com.google.gson.*;
import com.qiniu.common.QiniuException;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.service.line.FileInfoTableFormatter;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.JsonConvertUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FileStat extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    private String format;
    private IStringFormat<FileInfo> stringFormatter;

    public FileStat(Auth auth, Configuration configuration, String bucket, String resultPath, String format,
                    int resultIndex) throws IOException {
        super("stat", auth, configuration, bucket, resultPath, resultIndex);
        this.format = format;
        this.stringFormatter = new FileInfoTableFormatter("\t", null);
    }

    public FileStat(Auth auth, Configuration configuration, String bucket, String resultPath, String format)
            throws IOException {
        this(auth, configuration, bucket, resultPath, format, 0);
    }

    @Override
    public FileStat clone() throws CloneNotSupportedException {
        FileStat fileStat = (FileStat)super.clone();
        fileStat.stringFormatter = new FileInfoTableFormatter("\t", null);
        return fileStat;
    }

    public String getInputParams(Map<String, String> line) {
        return line.get("key");
    }

    public String processLine(Map<String, String> line) throws QiniuException {
        FileInfo fileInfo = bucketManager.stat(bucket, line.get("key"));
        fileInfo.key = line.get("key");
        if ("table".equals(format)) return stringFormatter.toFormatString(fileInfo);
        else return JsonConvertUtils.toJsonWithoutUrlEscape(fileInfo);
    }

    synchronized public BatchOperations getOperations(List<Map<String, String>> lineList) {
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
                        fileMap.writeSuccess(stringFormatter.toFormatString(
                                new Gson().fromJson(jsonObject.get("data"), FileInfo.class)));
                    else fileMap.writeSuccess(jsonObject.get("data").toString());
                else
                    fileMap.writeError(processList.get(j).get("key") + "\t" + jsonObject.toString());
            } else {
                fileMap.writeError(processList.get(j).get("key") + "\tempty stat result");
            }
        }
    }
}
