package com.qiniu.service.media;

import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.sdk.OperationManager;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.FileNameUtils;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.StringMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QiniuPfop implements ILineProcess<Map<String, String>>, Cloneable {

    final private String processName;
    final private String accessKey;
    final private String secretKey;
    final private Configuration configuration;
    private OperationManager operationManager;
    final private String bucket;
    final private StringMap pfopParams;
    final private String fopsIndex;
    final private String rmPrefix;
    public int retryTimes = 3;
    final private String savePath;
    private String saveTag;
    private int saveIndex;
    private FileMap fileMap;

    public QiniuPfop(String accessKey, String secretKey, Configuration configuration, String bucket, String pipeline,
                     String fopsIndex, String rmPrefix, String savePath, int saveIndex) throws IOException {
        this.processName = "pfop";
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.configuration = configuration;
        this.operationManager = new OperationManager(Auth.create(accessKey, secretKey), configuration);
        this.bucket = bucket;
        this.pfopParams = new StringMap().putNotEmpty("pipeline", pipeline);
        if (fopsIndex == null || "".equals(fopsIndex)) throw new IOException("please set the fopsIndex.");
        else this.fopsIndex = fopsIndex;
        this.rmPrefix = rmPrefix;
        this.savePath = savePath;
        this.saveTag = "";
        this.saveIndex = saveIndex;
        this.fileMap = new FileMap(savePath, processName, String.valueOf(saveIndex));
        this.fileMap.initDefaultWriters();
    }

    public QiniuPfop(String accessKey, String secretKey, Configuration configuration, String bucket, String pipeline,
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

    public QiniuPfop clone() throws CloneNotSupportedException {
        QiniuPfop qiniuPfop = (QiniuPfop)super.clone();
        qiniuPfop.operationManager = new OperationManager(Auth.create(accessKey, secretKey), configuration.clone());
        qiniuPfop.fileMap = new FileMap(savePath, processName, saveTag + String.valueOf(++saveIndex));
        try {
            qiniuPfop.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return qiniuPfop;
    }

    public void processLine(List<Map<String, String>> lineList, int retryTimes) throws IOException {
        String key;
        String persistentId;
        int retry;
        for (Map<String, String> line : lineList) {
            try {
                key = FileNameUtils.rmPrefix(rmPrefix, line.get("key"));
            } catch (IOException e) {
                fileMap.writeError(String.valueOf(line) + "\t" + e.getMessage(), false);
                continue;
            }
            retry = retryTimes;
            while (retry > 0) {
                try {
                    persistentId = operationManager.pfop(bucket, key, line.get(fopsIndex), pfopParams);
                    fileMap.writeSuccess(key + "\t" + persistentId, false);
                    retry = 0;
                } catch (QiniuException e) {
                    retry = HttpResponseUtils.checkException(e, retry);
                    if (retry < 1) {
                        HttpResponseUtils.writeLog(e, fileMap, line.get("key"));
                        if (retry == -1) throw e;
                    }
                }
            }
        }
    }

    public void processLine(List<Map<String, String>> lineList) throws IOException {
        processLine(lineList, retryTimes);
    }

    public void closeResource() {
        fileMap.closeWriters();
    }
}