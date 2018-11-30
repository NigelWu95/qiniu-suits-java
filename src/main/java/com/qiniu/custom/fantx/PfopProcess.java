package com.qiniu.custom.fantx;

import com.qiniu.common.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.sdk.OperationManager;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class PfopProcess implements ILineProcess<Map<String, String>>, Cloneable {

    public Auth auth;
    public Configuration configuration;
    public OperationManager operationManager;
    public String bucket;
    public String pipeline;
    public String processName;
    public boolean batch = true;
    public int retryCount = 3;
    public String resultFileDir;
    public FileMap fileMap;

    private void initBaseParams() {
        this.processName = "pfop";
    }

    public PfopProcess(Auth auth, Configuration configuration, String bucket, String pipeline, String resultFileDir) {
        this.auth = auth;
        this.configuration = configuration;
        this.operationManager = new OperationManager(auth, configuration);
        this.bucket = bucket;
        this.pipeline = pipeline;
        this.resultFileDir = resultFileDir;
        initBaseParams();
        this.fileMap = new FileMap();
    }

    public PfopProcess(Auth auth, Configuration configuration, String bucket, String pipeline, String resultFileDir,
                       int resultFileIndex) throws IOException {
        this(auth, configuration, bucket, pipeline, resultFileDir);
        this.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public PfopProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        PfopProcess qiniuPfop = (PfopProcess)super.clone();
        qiniuPfop.operationManager = new OperationManager(auth, configuration);
        qiniuPfop.fileMap = new FileMap();
        try {
            qiniuPfop.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return qiniuPfop;
    }

    public void setBatch(boolean batch) {
        this.batch = batch;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public String getProcessName() {
        return this.processName;
    }

    public String getInfo() {
        return bucket + "\t" + pipeline;
    }

    /**
     * 从转码成功的 mp4 进行 copy m3u8
     * @param lineList
     * @throws QiniuException
     */
    public void processLine(List<Map<String, String>> lineList) throws QiniuException {

        lineList = lineList == null ? null : lineList.parallelStream()
                .filter(Objects::nonNull).collect(Collectors.toList());
        if (lineList == null || lineList.size() == 0) return;
        List<String> resultList = new ArrayList<>();
        for (Map<String, String> line : lineList) {
            try {
                String key = line.get("0");
                String m3u8Key = ObjectUtils.replaceExt(key, "m3u8");
                String m3u8Copy = "avthumb/m3u8/vcodec/copy/acodec/copy|saveas/";
                String fop = m3u8Copy + UrlSafeBase64.encodeToString(bucket + ":" + m3u8Key);
                String persistentId = null;
                try {
                    persistentId = operationManager.pfop(bucket, key, fop, new StringMap()
                            .putNotEmpty("pipeline", pipeline));
                } catch (QiniuException e1) {
                    HttpResponseUtils.checkRetryCount(e1, retryCount);
                    while (retryCount > 0) {
                        try {
                            persistentId = operationManager.pfop(bucket, key, fop, new StringMap()
                                    .putNotEmpty("pipeline", pipeline));
                            retryCount = 0;
                        } catch (QiniuException e2) {
                            retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                        }
                    }
                } catch (Exception e) {
                    throw new QiniuException(e, e.getMessage());
                }

                if (persistentId != null && !"".equals(persistentId)) resultList.add(persistentId);
                else throw new QiniuException(null, "empty pfop persistent id");
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileMap, processName, getInfo() + "\t" + line.get("0"));
            }
        }
        if (resultList.size() > 0) fileMap.writeSuccess(String.join("\n", resultList));
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}