package com.qiniu.process.qos;

import com.qiniu.http.Client;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ChangeMetadata extends Base<Map<String, String>> {

    private Map<String, String> metadata;
    private ArrayList<String> ops;
    private List<Map<String, String>> lines;
    private Auth auth;
    private Configuration configuration;
    private Client client;
    private static final String URL = "http://rs.qiniu.com/batch";

    public ChangeMetadata(String accessKey, String secretKey, Configuration configuration, String bucket, Map<String, String> metadata)
            throws IOException {
        super("metadata", accessKey, secretKey, bucket);
        if (metadata == null) throw new IOException("metadata can not be null");
        this.metadata = metadata;
        CloudApiUtils.checkQiniu(accessKey, secretKey, configuration, bucket);
        this.auth = Auth.create(accessKey, secretKey);
        this.configuration = configuration;
        this.client = new Client(configuration.clone());
    }

    public ChangeMetadata(String accessKey, String secretKey, Configuration configuration, String bucket, Map<String, String> metadata,
                          String savePath, int saveIndex) throws IOException {
        super("metadata", accessKey, secretKey, bucket, savePath, saveIndex);
        if (metadata == null) throw new IOException("metadata can not be null");
        this.metadata = metadata;
        this.batchSize = 1000;
        this.ops = new ArrayList<>();
        this.lines = new ArrayList<>();
        CloudApiUtils.checkQiniu(accessKey, secretKey, configuration, bucket);
        this.auth = Auth.create(accessKey, secretKey);
        this.configuration = configuration;
        this.client = new Client(configuration.clone());
    }

    public ChangeMetadata(String accessKey, String secretKey, Configuration configuration, String bucket, Map<String, String> metadata,
                          String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, metadata, savePath, 0);
    }

    public ChangeMetadata clone() throws CloneNotSupportedException {
        ChangeMetadata changeType = (ChangeMetadata)super.clone();
        changeType.ops = new ArrayList<>();
        changeType.lines = new ArrayList<>();
        changeType.auth = Auth.create(accessId, secretKey);
        changeType.client = new Client(configuration.clone());
        return changeType;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get("key");
    }

    @Override
    protected List<Map<String, String>> putBatchOperations(List<Map<String, String>> processList) throws IOException {
        ops.clear();
        lines.clear();
        String key;
        String encodedMetaValue;
        String path;
        for (Map<String, String> map : processList) {
            key = map.get("key");
            if (key != null) {
                lines.add(map);
                path = String.format("/chgm/%s", BucketManager.encodedEntry(bucket, key));
                for (String k : metadata.keySet()) {
                    encodedMetaValue = UrlSafeBase64.encodeToString(metadata.get(k));
                    path = String.format("%s/x-qn-meta-!%s/%s", path, k, encodedMetaValue);
                }
                ops.add(path);
            } else {
                fileSaveMapper.writeError("key is not exists or empty in " + map, false);
            }
        }
        return lines;
    }

    @Override
    protected String batchResult(List<Map<String, String>> lineList) throws IOException {
        byte[] body = StringUtils.utf8Bytes(StringUtils.join(ops, "&op=", "op="));
        StringMap headers = auth.authorization(URL, body, Client.FormMime);
        return HttpRespUtils.getResult(client.post(URL, body, headers, Client.FormMime));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws IOException {
        String key = line.get("key");
        if (key == null) throw new IOException("key is not exists or empty in " + line);
        String path = String.format("/chgm/%s", BucketManager.encodedEntry(bucket, key));
        for (String k : metadata.keySet()) {
            path = String.format("%s/x-qn-meta-!%s/%s", path, k, UrlSafeBase64.encodeToString(metadata.get(k)));
        }
        String url = "http://rs.qiniu.com" + path;
        StringMap headers = auth.authorization(url, null, Client.FormMime);
        return String.join("\t", key, String.valueOf(metadata),
                HttpRespUtils.getResult(client.post(url, null, headers, Client.FormMime)));
    }

    @Override
    public void closeResource() {
        super.closeResource();
        metadata = null;
        ops = null;
        lines = null;
        auth = null;
        configuration = null;
        client = null;
    }
}
