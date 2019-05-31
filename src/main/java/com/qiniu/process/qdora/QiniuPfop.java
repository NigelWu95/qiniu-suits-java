package com.qiniu.process.qdora;

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
    private String fops;
    private OperationManager operationManager;

    public QiniuPfop(String accessKey, String secretKey, Configuration configuration, String bucket, String pipeline,
                     String pfopJsonPath, List<JsonObject> pfopConfigs, String fopsIndex, String fops, String savePath,
                     int saveIndex) throws IOException {
        super("pfop", accessKey, secretKey, configuration, bucket, savePath, saveIndex);
        set(pipeline, pfopJsonPath, pfopConfigs, fopsIndex, fops);
        this.operationManager = new OperationManager(Auth.create(accessKey, secretKey), configuration.clone());
    }

    public QiniuPfop(String accessKey, String secretKey, Configuration configuration, String bucket, String pipeline,
                     String pfopJsonPath, List<JsonObject> pfopConfigs, String fopsIndex, String fops) throws IOException {
        super("pfop", accessKey, secretKey, configuration, bucket);
        set(pipeline, pfopJsonPath, pfopConfigs, fopsIndex, fops);
        this.operationManager = new OperationManager(Auth.create(accessKey, secretKey), configuration.clone());
    }

    public QiniuPfop(String accessKey, String secretKey, Configuration configuration, String bucket, String pipeline,
                     String pfopJsonPath, List<JsonObject> pfopConfigs, String fopsIndex, String fops, String savePath)
            throws IOException {
        this(accessKey, secretKey, configuration, bucket, pipeline, pfopJsonPath, pfopConfigs, fopsIndex, fops, savePath, 0);
    }

    private void set(String pipeline, String jsonPath, List<JsonObject> pfopConfigs, String fopsIndex, String fops)
            throws IOException {
        this.pfopParams = new StringMap().putNotEmpty("pipeline", pipeline);
        if (jsonPath != null && !"".equals(jsonPath)) {
            this.pfopConfigs = new ArrayList<>();
            JsonFile jsonFile = new JsonFile(jsonPath);
            for (String key : jsonFile.getKeys()) {
                JsonObject jsonObject = PfopUtils.checkPfopJson(jsonFile.getElement(key).getAsJsonObject(), false);
                jsonObject.addProperty("name", key);
                this.pfopConfigs.add(jsonObject);
            }
        } else if (pfopConfigs != null && pfopConfigs.size() > 0) {
            this.pfopConfigs = pfopConfigs;
        } else if (fopsIndex != null && !"".equals(fopsIndex)) {
            this.fopsIndex = fopsIndex;
        } else if (fops != null && !"".equals(fops)) {
            this.fops = fops;
        } else {
            throw new IOException("please set the pfop-config or fopsIndex or fops.");
        }
    }

    public void updateFop(String bucket, String pipeline, String jsonPath, List<JsonObject> pfopConfigs, String fopsIndex,
                          String fops) throws IOException {
        this.bucket = bucket;
        set(pipeline, jsonPath, pfopConfigs, fopsIndex, fops);
    }

    public QiniuPfop clone() throws CloneNotSupportedException {
        QiniuPfop qiniuPfop = (QiniuPfop)super.clone();
        qiniuPfop.operationManager = new OperationManager(Auth.create(accessKey, secretKey), configuration.clone());
        return qiniuPfop;
    }

    @Override
    public String resultInfo(Map<String, String> line) {
        return line.get("key") + "\t" + line.get(fopsIndex);
    }

    @Override
    public boolean validCheck(Map<String, String> line) {
        return line.get("key") != null;
    }

    @Override
    public void parseSingleResult(Map<String, String> line, String result) {
    }

    @Override
    public String singleResult(Map<String, String> line) throws IOException {
        if (pfopConfigs != null) {
            for (JsonObject pfopConfig : pfopConfigs) {
                String cmd = PfopUtils.generateFopCmd(line.get("key"), pfopConfig);
                fileSaveMapper.writeKeyFile(pfopConfig.get("name").getAsString(), line.get("key") + "\t" + cmd + "\t" +
                            operationManager.pfop(bucket, line.get("key"), cmd, pfopParams), false);
            }
        } else if (fopsIndex != null && !"".equals(fopsIndex)) {
            fileSaveMapper.writeSuccess(line.get("key") + "\t" + line.get(fopsIndex) + "\t" +
                        operationManager.pfop(bucket, line.get("key"), line.get(fopsIndex), pfopParams), false);
        } else {
            fileSaveMapper.writeSuccess(line.get("key") + "\t" + fops + "\t" +
                    operationManager.pfop(bucket, line.get("key"), fops, pfopParams), false);
        }
        return null;
    }
}
