package com.qiniu.service.media;

import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.sdk.OperationManager;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.StringMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QiniuPfop implements ILineProcess<Map<String, String>>, Cloneable {

    final private String processName;
    final private String accessKey;
    final private String secretKey;
    final private Configuration configuration;
    private OperationManager operationManager;
    final private String bucket;
    final private String fopsIndex;
    final private StringMap pfopParams;
    public int retryCount;
    final private String savePath;
    private String saveTag;
    private int saveIndex;
    private FileMap fileMap;

    public QiniuPfop(String accessKey, String secretKey, Configuration configuration, String bucket, String pipeline,
                     String fopsIndex, String savePath, int saveIndex) throws IOException {
        this.processName = "pfop";
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.configuration = configuration;
        this.operationManager = new OperationManager(Auth.create(accessKey, secretKey), configuration);
        this.bucket = bucket;
        if (fopsIndex == null || "".equals(fopsIndex)) throw new IOException("please set the fopsIndex.");
        else this.fopsIndex = fopsIndex;
        this.pfopParams = new StringMap().putNotEmpty("pipeline", pipeline);
        this.savePath = savePath;
        this.saveTag = "";
        this.saveIndex = saveIndex;
        this.fileMap = new FileMap(savePath, processName, String.valueOf(saveIndex));
        this.fileMap.initDefaultWriters();
    }

    public QiniuPfop(String accessKey, String secretKey, Configuration configuration, String bucket, String pipeline,
                     String fopsIndex, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, pipeline, fopsIndex, savePath, 0);
    }

    public String getProcessName() {
        return this.processName;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount < 1 ? 1 : retryCount;
    }

    public void setSaveTag(String saveTag) {
        this.saveTag = saveTag == null ? "" : saveTag;
    }

    public QiniuPfop clone() throws CloneNotSupportedException {
        QiniuPfop qiniuPfop = (QiniuPfop)super.clone();
        qiniuPfop.operationManager = new OperationManager(Auth.create(accessKey, secretKey), configuration);
        qiniuPfop.fileMap = new FileMap(savePath, processName, saveTag + String.valueOf(++saveIndex));
        try {
            qiniuPfop.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return qiniuPfop;
    }

    public void processLine(List<Map<String, String>> lineList, int retryCount) throws IOException {
        String key;
        String persistentId = null;
        for (Map<String, String> line : lineList) {
            key = line.get("key");
            int retry = retryCount;
            while (retry > 0) {
                try {
                    persistentId = operationManager.pfop(bucket, key, line.get(fopsIndex), pfopParams);
                    retry = 0;
                } catch (QiniuException e) {
                    retry--;
                    HttpResponseUtils.processException(e, retry, fileMap,
                            new ArrayList<String>(){{ add(line.get("key") + "\t" + line.get(fopsIndex)); }});
                }
            }
            if (persistentId != null && !"".equals(persistentId))
                fileMap.writeSuccess(persistentId + "\t" + key, false);
            else fileMap.writeError( key + "\t" + line.get(fopsIndex) + "\tempty persistent id", false);
        }
    }

    public void processLine(List<Map<String, String>> lineList) throws IOException {
        processLine(lineList, retryCount);
    }

    public void closeResource() {
        fileMap.closeWriters();
    }
}