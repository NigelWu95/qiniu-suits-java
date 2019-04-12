package com.qiniu.process.qdora;

import com.google.gson.JsonObject;
import com.qiniu.common.QiniuException;
import com.qiniu.config.JsonFile;
import com.qiniu.process.Base;
import com.qiniu.sdk.OperationManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QiniuPfop extends Base {

    private StringMap pfopParams;
    private String fopsIndex;
    private ArrayList<JsonObject> pfopConfigs;
    private OperationManager operationManager;

    public QiniuPfop(String accessKey, String secretKey, Configuration configuration, String bucket, String pipeline,
                     String fopsIndex, String jsonPath, String savePath, int saveIndex) throws IOException {
        super("pfop", accessKey, secretKey, configuration, bucket, savePath, saveIndex);
        set(pipeline, fopsIndex, jsonPath);
        this.operationManager = new OperationManager(Auth.create(accessKey, secretKey), configuration.clone());
    }

    public void updateFop(String bucket, String pipeline, String fopsIndex, String jsonPath) throws IOException {
        this.bucket = bucket;
        set(pipeline, fopsIndex, jsonPath);
    }

    private void set(String pipeline, String fopsIndex, String jsonPath) throws IOException {
        this.pfopParams = new StringMap().putNotEmpty("pipeline", pipeline);
        if (fopsIndex == null || "".equals(fopsIndex)) throw new IOException("please set the fopsIndex.");
        else this.fopsIndex = fopsIndex;
        this.pfopConfigs = new ArrayList<>();
        if (jsonPath == null || "".equals(jsonPath)) return;
        JsonFile jsonFile = new JsonFile(jsonPath);
        for (String key : jsonFile.getKeys()) {
            JsonObject jsonObject = jsonFile.getElement(key).getAsJsonObject();
            if (!jsonObject.keySet().contains("cmd") || !jsonObject.keySet().contains("saveas"))
                throw new IOException(jsonPath + " miss the \"cmd\" or \"saveas\" fields in \"" + key + "\"");
            else if (!jsonObject.get("saveas").getAsString().contains(":"))
                throw new IOException(jsonPath + " miss the <bucket> field of \"saveas\" field in \"" + key + "\"");
            jsonObject.addProperty("name", key);
            this.pfopConfigs.add(jsonObject);
        }
    }

    public QiniuPfop(String accessKey, String secretKey, Configuration configuration, String bucket, String pipeline,
                     String fopsIndex, String jsonPath, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, pipeline, fopsIndex, jsonPath, savePath, 0);
    }

    public QiniuPfop clone() throws CloneNotSupportedException {
        QiniuPfop qiniuPfop = (QiniuPfop)super.clone();
        qiniuPfop.operationManager = new OperationManager(Auth.create(accessKey, secretKey), configuration.clone());
        return qiniuPfop;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get("key") + "\t" + line.get(fopsIndex);
    }

    @Override
    protected void parseSingleResult(Map<String, String> line, String result) throws IOException {
        fileMap.writeSuccess(result, false);
    }

    @Override
    protected String singleResult(Map<String, String> line) throws QiniuException {
        List<String> resultList = new ArrayList<>();
        for (JsonObject pfopConfig : pfopConfigs) {
            String cmd = PfopUtils.generateFopCmd(line.get("key"), pfopConfig);
            resultList.add(line.get("key") + "\t" + cmd + "\t" + operationManager.pfop(bucket, line.get("key"), cmd, pfopParams));
        }
        return String.join("\n", resultList);
    }
}
