package com.qiniu.process.qiniu;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.process.Base;
//import com.qiniu.storage.BucketManager;
//import com.qiniu.storage.BucketManager.*;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CopyFile extends Base<Map<String, String>> {

    private String toBucket;
    private String toKeyIndex;
    private String addPrefix;
    private String rmPrefix;
    private String forceOption;
    private boolean defaultToKey;
    private Auth auth;
    private Client client;
    private ArrayList<String> ops;
    private Configuration configuration;
    private List<Map<String, String>> lines;
//    private BucketManager bucketManager;
//    private BatchOperations batchOperations;

    public CopyFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String toKeyIndex, String addPrefix, String rmPrefix, boolean force) throws IOException {
        super("copy", accessKey, secretKey, bucket);
//        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration);
//        CloudApiUtils.checkQiniu(bucketManager, bucket);
//        CloudApiUtils.checkQiniu(bucketManager, toBucket);
        CloudApiUtils.checkQiniu(accessKey, secretKey, configuration, bucket);
        this.auth = Auth.create(accessKey, secretKey);
        this.client = new Client(configuration.clone());
        set(configuration, toBucket, toKeyIndex, addPrefix, rmPrefix, force);
    }

    public CopyFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String toKeyIndex, String addPrefix, String rmPrefix, boolean force, String savePath, int saveIndex)
            throws IOException {
        super("copy", accessKey, secretKey, bucket, savePath, saveIndex);
        this.batchSize = 1000;
        this.lines = new ArrayList<>(1000);
//        this.batchOperations = new BatchOperations();
//        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration);
//        CloudApiUtils.checkQiniu(bucketManager, bucket);
//        CloudApiUtils.checkQiniu(bucketManager, toBucket);
        CloudApiUtils.checkQiniu(accessKey, secretKey, configuration, bucket);
        this.batchSize = 1000;
        this.ops = new ArrayList<>(1000);
        this.lines = new ArrayList<>(1000);
        this.auth = Auth.create(accessKey, secretKey);
        this.client = new Client(configuration.clone());
        set(configuration, toBucket, toKeyIndex, addPrefix, rmPrefix, force);
    }

    public CopyFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String toKeyIndex, String keyPrefix, String rmPrefix, boolean force, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, toBucket, toKeyIndex, keyPrefix, rmPrefix, force, savePath, 0);
    }

    private void set(Configuration configuration, String toBucket, String toKeyIndex, String addPrefix, String rmPrefix,
                     boolean force) throws IOException {
        this.configuration = configuration;
        this.toBucket = toBucket;
        this.toKeyIndex = toKeyIndex;
        this.addPrefix = addPrefix == null ? "" : addPrefix;
        this.rmPrefix = rmPrefix;
        if (toKeyIndex == null || "".equals(toKeyIndex)) {
            this.toKeyIndex = "toKey"; // 没有传入的 toKeyIndex 参数的话直接设置为默认的 "toKey"
            defaultToKey = true;
            if (bucket.equals(toBucket) && (addPrefix == null || "".equals(addPrefix)) && (rmPrefix == null || "".equals(rmPrefix))) {
                throw new IOException("toBucket is same as bucket, but no toKeyIndex and no valid addPrefix or rmPrefix.");
            }
        }
        if (force) forceOption = "/force/true";
        else forceOption = "";
    }

    @Override
    public CopyFile clone() throws CloneNotSupportedException {
        CopyFile copyFile = (CopyFile)super.clone();
//        copyFile.bucketManager = new BucketManager(Auth.create(accessId, secretKey), configuration);
//        copyFile.batchOperations = new BatchOperations();
        if (fileSaveMapper != null) {
            copyFile.ops = new ArrayList<>(batchSize);
            copyFile.lines = new ArrayList<>(batchSize);
        }
        copyFile.auth = Auth.create(accessId, secretKey);
        copyFile.client = new Client(configuration.clone());
        return copyFile;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return String.join("\t", line.get("key"), line.get(toKeyIndex));
    }

    @Override
    protected List<Map<String, String>> putBatchOperations(List<Map<String, String>> processList) throws IOException {
//        batchOperations.clearOps();
        ops.clear();
        lines.clear();
        String key;
        String toKey;
        StringBuilder pathBuilder;
        if (defaultToKey) {
            for (Map<String, String> map : processList) {
                key = map.get("key");
                if (key != null) {
                    try {
                        toKey = String.join("", addPrefix, FileUtils.rmPrefix(rmPrefix, key));
                        map.put(toKeyIndex, toKey);
                        lines.add(map);
                        pathBuilder = new StringBuilder("/copy/")
                                .append(UrlSafeBase64.encodeToString(String.join(":", bucket, key))).append("/")
                                .append(UrlSafeBase64.encodeToString(String.join(":", toBucket, toKey)))
                                .append(forceOption);
                        ops.add(pathBuilder.toString());
                    } catch (IOException e) {
                        fileSaveMapper.writeError("no " + toKeyIndex + " in " + map, false);
                    }
                } else {
                    fileSaveMapper.writeError("key is not exists or empty in " + map, false);
                }
            }
        } else {
            for (Map<String, String> map : processList) {
                key = map.get("key");
                if (key != null) {
                    try {
                        toKey = String.join("", addPrefix, FileUtils.rmPrefix(rmPrefix, map.get(toKeyIndex)));
                        map.put(toKeyIndex, toKey);
                        lines.add(map);
                        pathBuilder = new StringBuilder("/copy/")
                                .append(UrlSafeBase64.encodeToString(String.join(":", bucket, key))).append("/")
                                .append(UrlSafeBase64.encodeToString(String.join(":", toBucket, toKey)))
                                .append(forceOption);
                        ops.add(pathBuilder.toString());
                    } catch (IOException e) {
                        fileSaveMapper.writeError("no " + toKeyIndex + " in " + map, false);
                    }
                } else {
                    fileSaveMapper.writeError("key is not exists or empty in " + map, false);
                }
            }
        }
        return lines;
    }

    @Override
    protected String batchResult(List<Map<String, String>> lineList) throws IOException {
//        return HttpRespUtils.getResult(bucketManager.batch(batchOperations));
        byte[] body = StringUtils.utf8Bytes(StringUtils.join(ops, "&op=", "op="));
        return HttpRespUtils.getResult(client.post(CloudApiUtils.QINIU_RS_BATCH_URL, body,
                auth.authorization(CloudApiUtils.QINIU_RS_BATCH_URL, body, Client.FormMime), Client.FormMime));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws IOException {
        String key = line.get("key");
        if (key == null) throw new IOException("key is not exists or empty in " + line);
        String toKey = addPrefix + FileUtils.rmPrefix(rmPrefix, defaultToKey ? key : line.get(toKeyIndex));
//        Response response = bucketManager.copy(bucket, key, toBucket, toKey, false);
        StringBuilder urlBuilder = new StringBuilder("http://rs.qiniu.com/copy/")
                .append(UrlSafeBase64.encodeToString(String.join(":", bucket, key))).append("/")
                .append(UrlSafeBase64.encodeToString(String.join(":", toBucket, toKey))).append(forceOption);
        StringMap headers = auth.authorization(urlBuilder.toString(), null, Client.FormMime);
        Response response = client.post(urlBuilder.toString(), null, headers, Client.FormMime);
        if (response.statusCode != 200) throw new QiniuException(response);
        response.close();
        return String.join("\t", key, toKey, "200");
    }

    @Override
    public void closeResource() {
        super.closeResource();
        toBucket = null;
        toKeyIndex = null;
        addPrefix = null;
        rmPrefix = null;
        if (ops != null) ops.clear();
        ops = null;
        if (lines != null) lines.clear();
        lines = null;
        auth = null;
        client = null;
        configuration = null;
//        bucketManager = null;
//        batchOperations = null;
    }
}
