package com.qiniu.process.qos;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ChangeMime extends Base<Map<String, String>> {

    private String mimeType;
    private String encodedMime;
    private String mimeIndex;
    private String condition;
    private String encodedCondition;
    private ArrayList<String> ops;
    private List<Map<String, String>> lines;
    private Auth auth;
    private Configuration configuration;
    private Client client;
    private static final String URL = "http://rs.qiniu.com/batch";

    public ChangeMime(String accessKey, String secretKey, Configuration configuration, String bucket, String mimeType,
                      String mimeIndex, String condition) throws IOException {
        super("mime", accessKey, secretKey, bucket);
        this.mimeType = mimeType;
        if (mimeType != null) encodedMime = UrlSafeBase64.encodeToString(mimeType);
        this.mimeIndex = mimeIndex == null ? "mime" : mimeIndex;
        this.condition = condition;
        if (condition != null) encodedCondition = UrlSafeBase64.encodeToString(condition);
        this.configuration = configuration;
        CloudApiUtils.checkQiniu(accessKey, secretKey, configuration, bucket);
        this.auth = Auth.create(accessKey, secretKey);
        this.configuration = configuration;
        this.client = new Client(configuration.clone());
    }

    public ChangeMime(String accessKey, String secretKey, Configuration configuration, String bucket, String mimeType,
                      String mimeIndex, String condition, String savePath, int saveIndex) throws IOException {
        super("mime", accessKey, secretKey, bucket, savePath, saveIndex);
        this.mimeType = mimeType;
        if (mimeType != null) encodedMime = UrlSafeBase64.encodeToString(mimeType);
        this.mimeIndex = mimeIndex == null ? "mime" : mimeIndex;
        this.condition = condition;
        if (condition != null) encodedCondition = UrlSafeBase64.encodeToString(condition);
        this.batchSize = 1000;
        this.ops = new ArrayList<>();
        this.lines = new ArrayList<>();
        CloudApiUtils.checkQiniu(accessKey, secretKey, configuration, bucket);
        this.auth = Auth.create(accessKey, secretKey);
        this.configuration = configuration;
        this.client = new Client(configuration.clone());
    }

    public ChangeMime(String accessKey, String secretKey, Configuration configuration, String bucket, String mimeType,
                      String mimeIndex, String condition, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, mimeType, mimeIndex, condition, savePath, 0);
    }

    public ChangeMime clone() throws CloneNotSupportedException {
        ChangeMime changeType = (ChangeMime)super.clone();
        changeType.auth = Auth.create(accessId, secretKey);
        changeType.client = new Client(configuration.clone());
        changeType.ops = new ArrayList<>();
        changeType.lines = new ArrayList<>();
        return changeType;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        if (mimeType == null) {
            return String.join("\t", line.get("key"), line.get(mimeIndex));
        } else {
            return line.get("key");
        }
    }

    @Override
    protected List<Map<String, String>> putBatchOperations(List<Map<String, String>> processList) throws IOException {
        ops.clear();
        lines.clear();
        String key;
        StringBuilder pathBuilder;
        if (mimeType == null) {
            String mime;
            for (Map<String, String> map : processList) {
                key = map.get("key");
                mime = map.get(mimeIndex);
                if (key != null && mime != null) {
                    lines.add(map);
                    pathBuilder = new StringBuilder("/chgm/")
                            .append(UrlSafeBase64.encodeToString(String.join(":", bucket, key)))
                            .append("/mime/").append(UrlSafeBase64.encodeToString(mime));
                    if (condition != null) pathBuilder.append("/cond/").append(encodedCondition);
                    ops.add(pathBuilder.toString());
//                    batchOperations.addChgmOp(bucket, mime, key);
                } else {
                    fileSaveMapper.writeError("key or mime is not exists or empty in " + map, false);
                }
            }
        } else {
            for (Map<String, String> map : processList) {
                key = map.get("key");
                if (key != null) {
                    lines.add(map);
                    pathBuilder = new StringBuilder("/chgm/")
                            .append(UrlSafeBase64.encodeToString(String.join(":", bucket, key)))
                            .append("/mime/").append(encodedMime);
                    if (condition != null) pathBuilder.append("/cond/").append(encodedCondition);
                    ops.add(pathBuilder.toString());
//                    batchOperations.addChgmOp(bucket, mimeType, key);
                } else {
                    fileSaveMapper.writeError("key is not exists or empty in " + map, false);
                }
            }
        }
        return lines;
    }

    @Override
    protected String batchResult(List<Map<String, String>> lineList) throws IOException {
        byte[] body = StringUtils.utf8Bytes(StringUtils.join(ops, "&op=", "op="));
        return HttpRespUtils.getResult(client.post(URL, body, auth.authorization(URL, body, Client.FormMime), Client.FormMime));
//        return HttpRespUtils.getResult(bucketManager.batch(batchOperations));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws IOException {
        String key = line.get("key");
        if (key == null) throw new IOException("key is not exists or empty in " + line);
        StringBuilder urlBuilder;
        if (mimeType == null) {
            String mime = line.get(mimeIndex);
            if (mime == null) throw new IOException("mime is not exists or empty in " + line);
            urlBuilder = new StringBuilder("http://rs.qiniu.com/chgm/")
                    .append(UrlSafeBase64.encodeToString(String.join(":", bucket, key)))
                    .append("/mime/").append(UrlSafeBase64.encodeToString(mime));
            if (condition != null) urlBuilder.append("/cond/").append(encodedCondition);
            String url = urlBuilder.toString();
            Response response = client.post(url, null, auth.authorization(url, null, Client.FormMime), Client.FormMime);
            if (response.statusCode != 200) throw new QiniuException(response);
            response.close();
            return String.join("\t", key, mime, "200");
        } else {
            urlBuilder = new StringBuilder("http://rs.qiniu.com/chgm/")
                    .append(UrlSafeBase64.encodeToString(String.join(":", bucket, key)))
                    .append("/mime/").append(encodedMime);
            if (condition != null) urlBuilder.append("/cond/").append(encodedCondition);
            String url = urlBuilder.toString();
            Response response = client.post(url, null, auth.authorization(url, null, Client.FormMime), Client.FormMime);
            if (response.statusCode != 200) throw new QiniuException(response);
            response.close();
            return String.join("\t", key, "200");
        }
    }

    @Override
    public void closeResource() {
        super.closeResource();
        mimeType = null;
        mimeIndex = null;
        condition = null;
//        batchOperations = null;
        if (ops != null) ops.clear();
        ops = null;
        if (lines != null) lines.clear();
        auth = null;
        configuration = null;
        client = null;
//        bucketManager = null;
    }
}
