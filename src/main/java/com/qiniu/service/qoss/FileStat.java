package com.qiniu.service.qoss;

import com.google.gson.*;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.service.line.FileInfoTableFormatter;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.JsonConvertUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    public void singleRun(List<Map<String, String>> fileInfoList, int retryCount) throws QiniuException {
        FileInfo fileInfo = null;
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        for (Map<String, String> line : fileInfoList) {
            int count = retryCount;
            try {
                while (count > 0) {
                    try {
                        fileInfo = bucketManager.stat(bucket, line.get("key"));
                        count = 0;
                    } catch (QiniuException e) {
                        count = HttpResponseUtils.getNextRetryCount(e, count);
                    }
                }
                if (fileInfo != null) {
                    fileInfo.key = line.get("key");
                    if ("table".equals(format)) fileMap.writeSuccess(stringFormatter.toFormatString(fileInfo));
                    else fileMap.writeSuccess(gson.toJson(fileInfo).replace("\\\\", "\\"));
                } else fileMap.writeError(line.get("key") + "\tempty stat result");
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileMap, new ArrayList<String>(){{add(line.get("key"));}});
            }
        }
    }

    @Override
    public void parseBatchResult(List<Map<String, String>> processList, String result) throws QiniuException {
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

    public void batchRun(List<Map<String, String>> fileInfoList, int retryCount) throws QiniuException {
        int times = fileInfoList.size()/1000 + 1;
        List<Map<String, String>> processList;
        Response response = null;
        String result;
        for (int i = 0; i < times; i++) {
            processList = fileInfoList.subList(1000 * i, i == times - 1 ? fileInfoList.size() : 1000 * (i + 1));
            if (processList.size() > 0) {
                batchOperations = getOperations(processList);
                int count = retryCount;
                while (count > 0) {
                    try {
                        response = bucketManager.batch(batchOperations);
                        count = 0;
                    } catch (QiniuException e1) {
                        count = HttpResponseUtils.getNextRetryCount(e1, count);
                    }
                }
                batchOperations.clearOps();
                try {
                    result = HttpResponseUtils.getResult(response);
                    parseBatchResult(processList, result);
                } catch (QiniuException e) {
                    HttpResponseUtils.processException(e, fileMap, processList.stream().map(line -> line.get("key"))
                            .collect(Collectors.toList()));
                }
            }
        }
    }
}
