package com.qiniu.process.qos;

import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.UploadManager;
import com.qiniu.storage.persistent.FileRecorder;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.Map;

public class UploadFile extends Base<Map<String, String>> {

    private Auth auth;
    private String pathIndex;
    private String parentPath;
    private boolean record;
    private StringMap params;
    private boolean checkCrc;
    private boolean keepPath;
    private String addPrefix;
    private String rmPrefix;
    private long expires;
    private StringMap policy;
    private Configuration configuration;
    private UploadManager uploadManager;

    public UploadFile(String accessKey, String secretKey, Configuration configuration, String bucket, String pathIndex,
                      String parentPath, boolean record, boolean keepPath, String addPrefix, String rmPrefix, long expires,
                      StringMap policy, StringMap params, boolean checkCrc, String savePath, int saveIndex) throws IOException {
        super("upload", accessKey, secretKey, null, savePath, saveIndex);
        auth = Auth.create(accessKey, secretKey);
        CloudApiUtils.checkQiniu(auth);
        uploadManager = record ? new UploadManager(configuration.clone(),
                new FileRecorder(savePath + FileUtils.pathSeparator + ".record"))
                : new UploadManager(configuration.clone());
        set(configuration, bucket, pathIndex, parentPath, record, keepPath, addPrefix, rmPrefix, expires, policy, params,
                checkCrc);
    }

    public UploadFile(String accessKey, String secretKey, Configuration configuration, String bucket, String pathIndex,
                      String parentPath, boolean record, boolean keepPath, String addPrefix, String rmPrefix, long expires,
                      StringMap policy, StringMap params, boolean checkCrc) throws IOException {
        super("upload", accessKey, secretKey, null);
        auth = Auth.create(accessKey, secretKey);
        CloudApiUtils.checkQiniu(auth);
        uploadManager = record ? new UploadManager(configuration.clone(),
                new FileRecorder(savePath + FileUtils.pathSeparator + ".record"))
                : new UploadManager(configuration.clone());
        set(configuration, bucket, pathIndex, parentPath, record, keepPath, addPrefix, rmPrefix, expires, policy, params,
                checkCrc);
    }

    public UploadFile(String accessKey, String secretKey, Configuration configuration, String bucket, String pathIndex,
                      String parentPath, boolean record, boolean keepPath, String addPrefix, String rmPrefix, long expires,
                      StringMap policy, StringMap params,
                      boolean checkCrc, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, pathIndex, parentPath, record, keepPath, addPrefix, rmPrefix,
                expires, policy, params, checkCrc, savePath, 0);
    }

    private void set(Configuration configuration, String bucket, String pathIndex, String parentPath, boolean record,
                     boolean keepPath, String addPrefix, String rmPrefix, long expires, StringMap policy, StringMap params,
                     boolean checkCrc) {
        this.configuration = configuration;
        this.bucket = bucket;
        if (pathIndex == null || "".equals(pathIndex)) this.pathIndex = "path";
        else this.pathIndex = pathIndex;
        this.parentPath = "".equals(parentPath) ? null : parentPath;
        this.record = record;
        this.keepPath = keepPath;
        this.addPrefix = addPrefix == null ? "" : addPrefix;
        this.rmPrefix = rmPrefix;
        this.expires = expires;
        this.policy = policy;
        this.params = params;
        this.checkCrc = checkCrc;
    }

    public UploadFile clone() throws CloneNotSupportedException {
        UploadFile downloadFile = (UploadFile)super.clone();
        downloadFile.auth = Auth.create(accessId, secretKey);
        try {
            downloadFile.uploadManager = record ? new UploadManager(configuration.clone(),
                    new FileRecorder(savePath + FileUtils.pathSeparator + ".record"))
                    : new UploadManager(configuration.clone());
        } catch (IOException e) {
            throw new CloneNotSupportedException(e.getMessage() + ", init writer failed.");
        }
        return downloadFile;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get("key") + "\t" + line.get(pathIndex);
    }

    @Override
    protected String singleResult(Map<String, String> line) throws Exception {
        String filepath = line.get(pathIndex);
        String key = line.get("key");
        if (filepath == null || "".equals(filepath)) {
            if (key == null || "".equals(key)) throw new IOException("filepath is not exists or empty in " + line);
            if (parentPath == null) filepath = key;
            else filepath = parentPath + FileUtils.pathSeparator + key;
            line.put(pathIndex, filepath);
            key = addPrefix + FileUtils.rmPrefix(rmPrefix, key);
        } else {
            if (key != null) {
                key = addPrefix + FileUtils.rmPrefix(rmPrefix, key);
            } else {
                key = keepPath ? filepath : filepath.substring(filepath.lastIndexOf(FileUtils.pathSeparator) + 1);
                if (key.startsWith(FileUtils.pathSeparator)) key = key.substring(1);
                key = addPrefix + FileUtils.rmPrefix(rmPrefix, key);
            }
            if (parentPath != null) filepath = parentPath + FileUtils.pathSeparator + filepath;
        }
        line.put("key", key);
        return HttpRespUtils.getResult(uploadManager.put(filepath, key, auth.uploadToken(bucket, key, expires, policy),
                params, null, checkCrc));
    }

    @Override
    public void closeResource() {
        super.closeResource();
        auth = null;
        pathIndex = null;
        addPrefix = null;
        rmPrefix = null;
        policy = null;
        params = null;
        configuration = null;
        uploadManager = null;
    }
}
