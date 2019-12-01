package com.qiniu.process.qos;

import com.google.gson.JsonObject;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
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
    private String recorder;
    private StringMap params;
    private boolean checkCrc;
    private boolean keepPath;
    private String addPrefix;
    private String rmPrefix;
    private long expires;
    private StringMap policy;
    private Configuration configuration;
    private UploadManager uploadManager;
    private boolean statCheck;
    private BucketManager bucketManager;

    public UploadFile(String accessKey, String secretKey, Configuration configuration, String bucket, String pathIndex,
                      String parentPath, boolean record, boolean keepPath, String addPrefix, String rmPrefix, long expires,
                      StringMap policy, StringMap params, boolean checkCrc, boolean statCheck, String savePath, int saveIndex)
            throws IOException {
        super("qupload", accessKey, secretKey, null, savePath, saveIndex);
        CloudApiUtils.checkQiniu(accessKey, secretKey, configuration, bucket);
        auth = Auth.create(accessKey, secretKey);
        if (record) {
            recorder = String.join(FileUtils.pathSeparator, savePath, ".record");
            uploadManager = new UploadManager(configuration.clone(), new FileRecorder(recorder));
        } else {
            uploadManager = new UploadManager(configuration.clone());
        }
        set(configuration, bucket, pathIndex, parentPath, keepPath, addPrefix, rmPrefix, expires, policy, params,
                checkCrc, statCheck);
    }

    public UploadFile(String accessKey, String secretKey, Configuration configuration, String bucket, String pathIndex,
                      String parentPath, boolean record, boolean keepPath, String addPrefix, String rmPrefix, long expires,
                      StringMap policy, StringMap params, boolean checkCrc, boolean statCheck) throws IOException {
        super("qupload", accessKey, secretKey, null);
        CloudApiUtils.checkQiniu(accessKey, secretKey, configuration, bucket);
        auth = Auth.create(accessKey, secretKey);
        if (record) {
            recorder = String.join(FileUtils.pathSeparator, FileUtils.userHome, ".qsuits.record");
            uploadManager = new UploadManager(configuration.clone(), new FileRecorder(recorder));
        } else {
            uploadManager = new UploadManager(configuration.clone());
        }
        set(configuration, bucket, pathIndex, parentPath, keepPath, addPrefix, rmPrefix, expires, policy, params,
                checkCrc, statCheck);
    }

    public UploadFile(String accessKey, String secretKey, Configuration configuration, String bucket, String pathIndex,
                      String parentPath, boolean record, boolean keepPath, String addPrefix, String rmPrefix, long expires,
                      StringMap policy, StringMap params, boolean checkCrc, boolean statCheck, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, pathIndex, parentPath, record, keepPath, addPrefix, rmPrefix,
                expires, policy, params, checkCrc, statCheck, savePath, 0);
    }

    private void set(Configuration configuration, String bucket, String pathIndex, String parentPath, boolean keepPath,
                     String addPrefix, String rmPrefix, long expires, StringMap policy, StringMap params, boolean checkCrc,
                     boolean statCheck) {
        this.configuration = configuration;
        this.bucket = bucket;
        if (pathIndex == null || "".equals(pathIndex)) this.pathIndex = "filepath";
        else this.pathIndex = pathIndex;
        this.parentPath = "".equals(parentPath) ? null : parentPath;
        if (this.parentPath != null && this.parentPath.endsWith(FileUtils.pathSeparator)) {
            this.parentPath = this.parentPath.substring(0, parentPath.length() - 1);
        }
        this.keepPath = keepPath;
        this.addPrefix = addPrefix == null ? "" : addPrefix;
        this.rmPrefix = rmPrefix;
        this.expires = expires;
        this.policy = policy;
        this.params = params;
        this.checkCrc = checkCrc;
        this.statCheck = statCheck;
        if (statCheck) bucketManager = new BucketManager(auth, configuration.clone());
    }

    public UploadFile clone() throws CloneNotSupportedException {
        UploadFile uploadFile = (UploadFile)super.clone();
        uploadFile.auth = Auth.create(accessId, secretKey);
        try {
            uploadFile.uploadManager = recorder == null ? new UploadManager(configuration.clone()) :
                    new UploadManager(configuration.clone(), new FileRecorder(recorder));
            if (statCheck) uploadFile.bucketManager = new BucketManager(auth, configuration.clone());
        } catch (IOException e) {
            throw new CloneNotSupportedException(e.getMessage() + ", init writer failed.");
        }
        return uploadFile;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return String.join( "\t", line.get("key"), line.get(pathIndex));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws Exception {
        String filepath = line.get(pathIndex);
        String key = line.get("key");
        if (filepath == null || "".equals(filepath)) {
            if (key == null || "".equals(key)) throw new IOException(pathIndex + " is not exists or empty in " + line);
            if (parentPath == null) {
                filepath = key;
                if (key.startsWith(FileUtils.pathSeparator)) key = key.substring(1);
            } else {
                if (key.startsWith(FileUtils.pathSeparator)) {
                    filepath = String.join("", parentPath, key);
                    key = key.substring(1);
                } else {
                    filepath = String.join(FileUtils.pathSeparator, parentPath, key);
                }
            }
            line.put(pathIndex, filepath);
        } else {
            if (key != null) {
                if (keepPath) {
                    if (key.startsWith(FileUtils.pathSeparator)) key = key.substring(1);
                } else {
                    key = key.substring(key.lastIndexOf(FileUtils.pathSeparator) + 1);
                }
            } else {
                if (keepPath) {
                    if (filepath.startsWith(FileUtils.pathSeparator)) key = filepath.substring(1);
                    else key = filepath;
                } else {
                    key = filepath.substring(filepath.lastIndexOf(FileUtils.pathSeparator) + 1);
                }
            }
            if (parentPath != null) {
                if (filepath.startsWith(FileUtils.pathSeparator)) {
                    filepath = String.join("", parentPath, filepath);
                } else {
                    filepath = String.join( FileUtils.pathSeparator , parentPath, filepath);
                }
                line.put(pathIndex, filepath);
            }
        }
        key = String.join("", addPrefix, FileUtils.rmPrefix(rmPrefix, key));
        line.put("key", key);
        if (statCheck) {
            try {
                Response response = bucketManager.statResponse(bucket, key);
                JsonObject statJson = JsonUtils.toJsonObject(response.bodyString());
                statJson.addProperty("key", key);
                response.close();
                return String.join("\t", filepath, statJson.toString());
            } catch (QiniuException ignored) {}
        }
        if (filepath.endsWith(FileUtils.pathSeparator)) {
            return String.join("\t", filepath, HttpRespUtils.getResult(uploadManager.put(new byte[]{}, key, auth.uploadToken(bucket, key, expires, policy),
                    params, null, checkCrc)));
        } else {
            return String.join("\t", filepath, HttpRespUtils.getResult(uploadManager.put(filepath, key, auth.uploadToken(bucket, key, expires, policy),
                    params, null, checkCrc)));
        }
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
