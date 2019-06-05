package com.qiniu.process.qdora;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.qiniu.config.JsonFile;
import com.qiniu.process.Base;
import com.qiniu.sdk.OperationManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QiniuPfop extends Base<Map<String, String>> {

    private StringMap pfopParams;
    private List<JsonObject> pfopConfigs;
    private String fopsIndex;
    private OperationManager operationManager;

    public QiniuPfop(String accessKey, String secretKey, Configuration configuration, String bucket, String pipeline,
                     String pfopJsonPath, List<JsonObject> pfopConfigs, String fopsIndex, String savePath,
                     int saveIndex) throws IOException {
        super("pfop", accessKey, secretKey, configuration, bucket, savePath, saveIndex);
        set(pipeline, pfopJsonPath, pfopConfigs, fopsIndex);
        this.operationManager = new OperationManager(Auth.create(accessKey, secretKey), configuration.clone());
    }

    public QiniuPfop(String accessKey, String secretKey, Configuration configuration, String bucket, String pipeline,
                     String pfopJsonPath, List<JsonObject> pfopConfigs, String fopsIndex) throws IOException {
        super("pfop", accessKey, secretKey, configuration, bucket);
        set(pipeline, pfopJsonPath, pfopConfigs, fopsIndex);
        this.operationManager = new OperationManager(Auth.create(accessKey, secretKey), configuration.clone());
    }

    public QiniuPfop(String accessKey, String secretKey, Configuration configuration, String bucket, String pipeline,
                     String pfopJsonPath, List<JsonObject> pfopConfigs, String fopsIndex, String savePath)
            throws IOException {
        this(accessKey, secretKey, configuration, bucket, pipeline, pfopJsonPath, pfopConfigs, fopsIndex, savePath, 0);
    }

    private void set(String pipeline, String pfopJsonPath, List<JsonObject> pfopConfigs, String fopsIndex) throws IOException {
        this.pfopParams = new StringMap().putNotEmpty("pipeline", pipeline);
        if (pfopConfigs != null && pfopConfigs.size() > 0) {
            this.pfopConfigs = pfopConfigs;
        } else if (pfopJsonPath != null && !"".equals(pfopJsonPath)) {
            this.pfopConfigs = new ArrayList<>();
            JsonFile jsonFile = new JsonFile(pfopJsonPath);
            JsonArray array = jsonFile.getElement("pfop").getAsJsonArray();
            for (JsonElement jsonElement : array) {
                JsonObject jsonObject = PfopUtils.checkPfopJson(jsonElement.getAsJsonObject(), false);
                this.pfopConfigs.add(jsonObject);
            }
        } else if (fopsIndex != null && !"".equals(fopsIndex)) {
            this.fopsIndex = fopsIndex;
        } else {
            throw new IOException("please set the pfop-config or fops-index.");
        }
    }

    public void updateFop(String bucket, String pipeline, String jsonPath, List<JsonObject> pfopConfigs, String fopsIndex)
            throws IOException {
        this.bucket = bucket;
        set(pipeline, jsonPath, pfopConfigs, fopsIndex);
    }

    public QiniuPfop clone() throws CloneNotSupportedException {
        QiniuPfop qiniuPfop = (QiniuPfop)super.clone();
        qiniuPfop.operationManager = new OperationManager(Auth.create(accessKey, secretKey), configuration.clone());
        return qiniuPfop;
    }

    @Override
    public String resultInfo(Map<String, String> line) {
        return line.get("key");
    }

    @Override
    public boolean validCheck(Map<String, String> line) {
        return line.get("key") != null;
    }

    @Override
    public String singleResult(Map<String, String> line) throws IOException {
        String key = line.get("key");
        if (pfopConfigs != null && pfopConfigs.size() > 0) {
            StringBuilder cmdBuilder = new StringBuilder();
            for (JsonObject pfopConfig : pfopConfigs) {
                cmdBuilder.append(pfopConfig.get("cmd").getAsString())
                        .append("|saveas/")
                        .append(UrlSafeBase64.encodeToString(PfopUtils.generateFopSaveAs(key, pfopConfig)))
                        .append(";");
            }
            cmdBuilder.deleteCharAt(cmdBuilder.length() - 1);
            return key + "\t" + operationManager.pfop(bucket, key, cmdBuilder.toString(), pfopParams);
        } else {
            return key + "\t" + operationManager.pfop(bucket, line.get("key"), line.get(fopsIndex), pfopParams);
        }
    }
}
