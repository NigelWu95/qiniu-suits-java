package com.qiniu.process.qos;

import com.google.gson.*;
import com.qiniu.convert.JsonToString;
import com.qiniu.convert.QOSObjToString;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpRespUtils;
import com.qiniu.util.CloudAPIUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class StatFile extends Base<Map<String, String>> {

    private String format;
    private String separator;
    private ITypeConvert typeConverter;
    private BatchOperations batchOperations;
    private Configuration configuration;
    private BucketManager bucketManager;

    public StatFile(String accessKey, String secretKey, Configuration configuration, String bucket, String format,
                    String separator) throws IOException {
        super("stat", accessKey, secretKey, bucket);
        set(configuration, format, separator);
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        CloudAPIUtils.checkQiniu(bucketManager, bucket);
    }

    public StatFile(String accessKey, String secretKey, Configuration configuration, String bucket, String savePath,
                    String format, String separator, int saveIndex) throws IOException {
        super("stat", accessKey, secretKey, bucket, savePath, saveIndex);
        set(configuration, format, separator);
        this.batchSize = 1000;
        this.batchOperations = new BatchOperations();
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        CloudAPIUtils.checkQiniu(bucketManager, bucket);
    }

    public StatFile(String accessKey, String secretKey, Configuration configuration, String bucket, String savePath,
                    String format, String separator) throws IOException {
        this(accessKey, secretKey, configuration, bucket, savePath, format, separator, 0);
    }

    private void set(Configuration configuration, String format, String separator) throws IOException {
        this.configuration = configuration;
        this.format = format;
        if ("csv".equals(format) || "tab".equals(format)) {
            this.separator = "csv".equals(format) ? "," : separator;
        } else if (!"json".equals(this.format)) {
            throw new IOException("please check your format for converting result string.");
        }
        if (batchSize > 1) typeConverter = new JsonToString(format, separator, null);
        else typeConverter = new QOSObjToString(format, separator, null);
    }

    public void updateFormat(String format) {
        this.format = format;
    }

    public void updateSeparator(String separator) {
        this.separator = separator;
    }

    public StatFile clone() throws CloneNotSupportedException {
        StatFile statFile = (StatFile)super.clone();
        statFile.bucketManager = new BucketManager(Auth.create(authKey1, authKey2), configuration.clone());
        statFile.batchOperations = new BatchOperations();
        statFile.errorLineList = new ArrayList<>();
        if (batchSize > 1) {
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
    synchronized protected List<Map<String, String>> putBatchOperations(List<Map<String, String>> processList) {
        batchOperations.clearOps();
        Iterator<Map<String, String>> iterator = processList.iterator();
        Map<String, String> line;
        String key;
        while (iterator.hasNext()) {
            line = iterator.next();
            key = line.get("key");
            if (key != null) {
                batchOperations.addStatOps(bucket, key);
            } else {
                iterator.remove();
                errorLineList.add("no key in " + line);
            }
        }
        return processList;
    }

    @Override
    public String batchResult(List<Map<String, String>> lineList) throws IOException {
        return HttpRespUtils.getResult(bucketManager.batch(batchOperations));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected List<Map<String, String>> parseBatchResult(List<Map<String, String>> processList, String result) throws Exception {
        if (result == null || "".equals(result)) throw new IOException("not valid json.");
        List<Map<String, String>> retryList = new ArrayList<>();
        JsonArray jsonArray = new Gson().fromJson(result, JsonArray.class);
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
                switch (HttpRespUtils.checkStatusCode(jsonObject.get("code").getAsInt())) {
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
    @SuppressWarnings("unchecked")
    protected String singleResult(Map<String, String> line) throws IOException {
        String key = line.get("key");
        if (key == null) throw new IOException("no key in " + line);
        FileInfo fileInfo = bucketManager.stat(bucket, key);
        fileInfo.key = key;
        return (String) typeConverter.convertToV(fileInfo);
    }

    @Override
    public void closeResource() {
        super.closeResource();
        format = null;
        separator = null;
        typeConverter = null;
        batchOperations = null;
        configuration = null;
        bucketManager = null;
    }
}
