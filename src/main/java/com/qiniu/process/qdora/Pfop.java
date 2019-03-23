package com.qiniu.process.qdora;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.process.Base;
import com.qiniu.sdk.OperationManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Pfop extends Base {

    private OperationManager operationManager;
    final private StringMap pfopParams;
    final private String fopsIndex;

    public Pfop(String accessKey, String secretKey, Configuration configuration, String bucket, String pipeline,
                String fopsIndex, String rmPrefix, String savePath, int saveIndex) throws IOException {
        super("pfop", accessKey, secretKey, configuration, bucket, rmPrefix, savePath, saveIndex);
        this.operationManager = new OperationManager(Auth.create(accessKey, secretKey), configuration);
        this.pfopParams = new StringMap().putNotEmpty("pipeline", pipeline);
        if (fopsIndex == null || "".equals(fopsIndex)) throw new IOException("please set the fopsIndex.");
        else this.fopsIndex = fopsIndex;
    }

    public Pfop(String accessKey, String secretKey, Configuration configuration, String bucket, String pipeline,
                String fopsIndex, String rmPrefix, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, pipeline, fopsIndex, rmPrefix, savePath, 0);
    }

    public String getProcessName() {
        return this.processName;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes < 1 ? 3 : retryTimes;
    }

    public void setSaveTag(String saveTag) {
        this.saveTag = saveTag == null ? "" : saveTag;
    }

    public Pfop clone() throws CloneNotSupportedException {
        Pfop qiniuPfop = (Pfop)super.clone();
        qiniuPfop.operationManager = new OperationManager(Auth.create(accessKey, secretKey), configuration.clone());
        return qiniuPfop;
    }

    protected Map<String, String> formatLine(Map<String, String> line) throws IOException {
        line.put("key", FileNameUtils.rmPrefix(rmPrefix, line.get("key"))
                    .replaceAll("\\?", "%3F"));
        return line;
    }

    protected String resultInfo(Map<String, String> line) {
        return line.get("key") + "\t" + line.get(fopsIndex);
    }

    protected Response batchResult(List<Map<String, String>> lineList) {
        return null;
    }

    protected String singleResult(Map<String, String> line) throws QiniuException {
        return operationManager.pfop(bucket, line.get("key"), line.get(fopsIndex), pfopParams);
    }
}
