package com.qiniu.process.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.persistence.FileMap;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.FileNameUtils;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.LogUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MirrorFetch implements ILineProcess<Map<String, String>>, Cloneable {

    final private String accessKey;
    final private String secretKey;
    final private Configuration configuration;
    private BucketManager bucketManager;
    final private String bucket;
    final private String processName;
    final private String rmPrefix;
    private int retryTimes = 3;
    final private String savePath;
    private String saveTag;
    private int saveIndex;
    private FileMap fileMap;

    public MirrorFetch(String accessKey, String secretKey, Configuration configuration, String bucket, String rmPrefix,
                       String savePath, int saveIndex) throws IOException {
        this.processName = "mirror";
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.configuration = configuration;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration);
        this.bucket = bucket;
        this.rmPrefix = rmPrefix;
        this.savePath = savePath;
        this.saveTag = "";
        this.saveIndex = saveIndex;
        this.fileMap = new FileMap(savePath, processName, String.valueOf(saveIndex));
        this.fileMap.initDefaultWriters();
    }

    public MirrorFetch(String accessKey, String secretKey, Configuration configuration, String bucket, String rmPrefix,
                       String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, rmPrefix, savePath, 0);
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

    public MirrorFetch clone() throws CloneNotSupportedException {
        MirrorFetch mirrorFetch = (MirrorFetch) super.clone();
        mirrorFetch.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        mirrorFetch.fileMap = new FileMap(savePath, processName, saveTag + String.valueOf(++saveIndex));
        try {
            mirrorFetch.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return mirrorFetch;
    }

    public void processLine(List<Map<String, String>> lineList, int retryTimes) throws IOException {
        int retry;
        String key;
        Map<String, String> line;
        for (int i = 0; i < lineList.size(); i++) {
            line = lineList.get(i);
            try {
                key = FileNameUtils.rmPrefix(rmPrefix, line.get("key"));
            } catch (IOException e) {
                LogUtils.writeLog(e, fileMap, line.get("key"));
                continue;
            }
            retry = retryTimes;
            while (retry > 0) {
                try {
                    bucketManager.prefetch(bucket, key);
                    fileMap.writeSuccess(line.get("key") + "\t" + "200", false);
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

    public void processLine(List<Map<String, String>> fileInfoList) throws IOException {
        processLine(fileInfoList, retryTimes);
    }

    public void closeResource() {
        fileMap.closeWriters();
    }
}
