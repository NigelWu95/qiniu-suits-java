package com.qiniu.process.qdora;

import com.qiniu.common.QiniuException;
import com.qiniu.process.Base;
import com.qiniu.sdk.OperationManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.Map;

public class QiniuPfop extends Base {

    private OperationManager operationManager;
    private StringMap pfopParams;
    private String fopsIndex;

    public QiniuPfop(String accessKey, String secretKey, Configuration configuration, String bucket, String pipeline,
                     String fopsIndex, String rmPrefix, String savePath, int saveIndex) throws IOException {
        super("pfop", accessKey, secretKey, configuration, bucket, rmPrefix, savePath, saveIndex);
        this.operationManager = new OperationManager(Auth.create(accessKey, secretKey), configuration.clone());
        this.pfopParams = new StringMap().putNotEmpty("pipeline", pipeline);
        if (fopsIndex == null || "".equals(fopsIndex)) throw new IOException("please set the fopsIndex.");
        else this.fopsIndex = fopsIndex;
    }

    public void updateFop(String bucket, String pipeline, String fopsIndex, String rmPrefix) throws IOException {
        this.bucket = bucket;
        this.pfopParams = new StringMap().putNotEmpty("pipeline", pipeline);
        if (fopsIndex == null || "".equals(fopsIndex)) throw new IOException("please set the fopsIndex.");
        else this.fopsIndex = fopsIndex;
        this.rmPrefix = rmPrefix;
    }

    public QiniuPfop(String accessKey, String secretKey, Configuration configuration, String bucket, String pipeline,
                     String fopsIndex, String rmPrefix, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, pipeline, fopsIndex, rmPrefix, savePath, 0);
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
    protected String singleResult(Map<String, String> line) throws QiniuException {
        return operationManager.pfop(bucket, line.get("key"), line.get(fopsIndex), pfopParams);
    }
}
