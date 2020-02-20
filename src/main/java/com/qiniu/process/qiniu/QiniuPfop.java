package com.qiniu.process.qiniu;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.qiniu.config.JsonFile;
import com.qiniu.process.Base;
import com.qiniu.processing.OperationManager;
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
    private Configuration configuration;
    private OperationManager operationManager;

    public QiniuPfop(String accessKey, String secretKey, Configuration configuration, String bucket, String pipeline,
                     String notifyURL, boolean force, String pfopJsonPath, List<JsonObject> pfopConfigs,
                     String fopsIndex) throws IOException {
        super("pfop", accessKey, secretKey, bucket);
        set(configuration, pipeline, notifyURL, force, pfopJsonPath, pfopConfigs, fopsIndex);
        this.operationManager = new OperationManager(Auth.create(accessKey, secretKey), configuration);
        CloudApiUtils.checkQiniu(accessKey, secretKey, configuration, bucket);
    }

    public QiniuPfop(String accessKey, String secretKey, Configuration configuration, String bucket, String pipeline,
                     String notifyURL, boolean force, String pfopJsonPath, List<JsonObject> pfopConfigs,
                     String fopsIndex, String savePath, int saveIndex) throws IOException {
        super("pfop", accessKey, secretKey, bucket, savePath, saveIndex);
        set(configuration, pipeline, notifyURL, force, pfopJsonPath, pfopConfigs, fopsIndex);
        this.operationManager = new OperationManager(Auth.create(accessKey, secretKey), configuration);
        CloudApiUtils.checkQiniu(accessKey, secretKey, configuration, bucket);
    }

    public QiniuPfop(String accessKey, String secretKey, Configuration configuration, String bucket, String pipeline,
                     String notifyURL, boolean force, String pfopJsonPath, List<JsonObject> pfopConfigs,
                     String fopsIndex, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, pipeline, notifyURL, force, pfopJsonPath, pfopConfigs,
                fopsIndex, savePath, 0);
    }

    private void set(Configuration configuration, String pipeline, String notifyURL, boolean force, String pfopJsonPath,
                     List<JsonObject> pfopConfigs, String fopsIndex) throws IOException {
        this.configuration = configuration;
        this.pfopParams = new StringMap()
                .putNotEmpty("pipeline", pipeline)
                .putNotEmpty("notifyURL", notifyURL)
                .putWhen("force", 1, force);
        if (pfopConfigs != null && pfopConfigs.size() > 0) {
            this.pfopConfigs = pfopConfigs;
        } else if (pfopJsonPath != null && !"".equals(pfopJsonPath)) {
            JsonFile jsonFile = new JsonFile(pfopJsonPath);
            JsonArray array = jsonFile.getElement("pfop").getAsJsonArray();
            this.pfopConfigs = new ArrayList<>(array.size());
            for (JsonElement jsonElement : array) {
                JsonObject jsonObject = PfopUtils.checkPfopJson(jsonElement.getAsJsonObject(), false);
                this.pfopConfigs.add(jsonObject);
            }
            if (this.pfopConfigs.size() <= 0) throw new IOException("please check pfop config json in: " + pfopJsonPath);
        } else if (fopsIndex != null && !"".equals(fopsIndex)) {
            this.fopsIndex = fopsIndex;
        } else {
            throw new IOException("please set the pfop-config or fops-index.");
        }
    }

    @Override
    public QiniuPfop clone() throws CloneNotSupportedException {
        QiniuPfop qiniuPfop = (QiniuPfop)super.clone();
        qiniuPfop.operationManager = new OperationManager(Auth.create(accessId, secretKey), configuration);
        return qiniuPfop;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        if (pfopConfigs == null) {
            return String.join("\t", line.get("key"), line.get(fopsIndex));
        } else {
            return line.get("key");
        }
    }

    @Override
    protected String singleResult(Map<String, String> line) throws Exception {
        String key = line.get("key");
        if (key == null) throw new IOException("key is not exists or empty in " + line);
        if (pfopConfigs == null) {
            String fops = line.get(fopsIndex);
            if (fops == null) throw new IOException("fops is not exists or empty in " + line);
            return String.join("\t", key, operationManager.pfop(bucket, key, line.get(fopsIndex), pfopParams));
        } else {
            StringBuilder cmdBuilder = new StringBuilder();
            for (JsonObject pfopConfig : pfopConfigs) {
                cmdBuilder.append(pfopConfig.get("cmd").getAsString())
                        .append("|saveas/")
                        .append(UrlSafeBase64.encodeToString(PfopUtils.generateFopSaveAs(key, pfopConfig)))
                        .append(";");
            }
            cmdBuilder.deleteCharAt(cmdBuilder.length() - 1);
            return String.join("\t", key, operationManager.pfop(bucket, key, cmdBuilder.toString(), pfopParams));
        }
    }

    @Override
    public void closeResource() {
        super.closeResource();
        pfopParams = null;
        pfopConfigs = null;
        fopsIndex = null;
        configuration = null;
        operationManager = null;
    }
}
