package com.qiniu.service.qdora;

import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.sdk.OperationManager;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

import java.io.IOException;
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
        Map<String, String> line;
        for (int i = 0; i < lineList.size(); i++) {
            line = lineList.get(i);
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
                    if (retry == 0) LogUtils.writeLog(e, fileMap, line.get("key"));
                    else if (retry == -1) {
                        LogUtils.writeLog(e, fileMap, lineList.subList(i, lineList.size() - 1).parallelStream()
                                .map(String::valueOf).collect(Collectors.toList()));
                        throw e;
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