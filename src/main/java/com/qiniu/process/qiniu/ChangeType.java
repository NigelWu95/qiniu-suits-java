package com.qiniu.process.qiniu;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
//import com.qiniu.storage.model.StorageType;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ChangeType extends Base<Map<String, String>> {

//    private StorageType storageType;
    private int type;
    private ArrayList<String> ops;
    private List<Map<String, String>> lines;
    private Auth auth;
    private Configuration configuration;
    private Client client;
//    private BucketManager bucketManager;

    public ChangeType(String accessKey, String secretKey, Configuration configuration, String bucket, int type) throws IOException {
        super("type", accessKey, secretKey, bucket);
//        storageType = type == 0 ? StorageType.COMMON : StorageType.INFREQUENCY;
        CloudApiUtils.checkQiniu(accessKey, secretKey, configuration, bucket);
        this.type = type;
        this.configuration = configuration;
        this.auth = Auth.create(accessKey, secretKey);
        this.client = new Client(configuration.clone());
//        this.bucketManager = new BucketManager(auth, configuration);
//        CloudApiUtils.checkQiniu(bucketManager, bucket);
    }

    public ChangeType(String accessKey, String secretKey, Configuration configuration, String bucket, int type, String savePath,
                      int saveIndex) throws IOException {
        super("type", accessKey, secretKey, bucket, savePath, saveIndex);
//        storageType = type == 0 ? StorageType.COMMON : StorageType.INFREQUENCY;
        CloudApiUtils.checkQiniu(accessKey, secretKey, configuration, bucket);
        this.type = type;
        this.batchSize = 1000;
        this.ops = new ArrayList<>();
        this.lines = new ArrayList<>();
        this.configuration = configuration;
        this.auth = Auth.create(accessKey, secretKey);
        this.client = new Client(configuration.clone());
//        this.batchOperations = new BatchOperations();
//        this.bucketManager = new BucketManager(auth, configuration);
//        CloudApiUtils.checkQiniu(bucketManager, bucket);
    }

    public ChangeType(String accessKey, String secretKey, Configuration configuration, String bucket, int type,
                      String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, type, savePath, 0);
    }

    @Override
    public ChangeType clone() throws CloneNotSupportedException {
        ChangeType changeType = (ChangeType)super.clone();
//        changeType.bucketManager = new BucketManager(changeType.auth, configuration);
//        changeType.batchOperations = new BatchOperations();
        if (fileSaveMapper != null) {
            changeType.ops = new ArrayList<>();
            changeType.lines = new ArrayList<>();
        }
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
//        batchOperations.clearOps();
//        lines.clear();
//        String key;
//        for (Map<String, String> map : processList) {
//            key = map.get("key");
//            if (key != null) {
//                lines.add(map);
//                batchOperations.addChangeTypeOps(bucket, storageType, key);
//            } else {
//                fileSaveMapper.writeError("key is not exists or empty in " + map, false);
//            }
//        }
//        return lines;
        ops.clear();
        lines.clear();
        String key;
//        String encodedMetaValue;
//        String path;
        StringBuilder pathBuilder;
        for (Map<String, String> map : processList) {
            key = map.get("key");
            if (key != null) {
                lines.add(map);
                pathBuilder = new StringBuilder("/chtype/")
                        .append(UrlSafeBase64.encodeToString(String.join(":", bucket, key)))
                        .append("/type/").append(type);
                ops.add(pathBuilder.toString());
            } else {
                fileSaveMapper.writeError("key is not exists or empty in " + map, false);
            }
        }
        return lines;
    }

    @Override
    protected String batchResult(List<Map<String, String>> lineList) throws IOException {
        byte[] body = StringUtils.utf8Bytes(StringUtils.join(ops, "&op=", "op="));
        return HttpRespUtils.getResult(client.post(CloudApiUtils.QINIU_RS_BATCH_URL, body,
                auth.authorization(CloudApiUtils.QINIU_RS_BATCH_URL, body, Client.FormMime), Client.FormMime));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws IOException {
        String key = line.get("key");
        if (key == null) throw new IOException("key is not exists or empty in " + line);
//        String path = String.format("/chgm/%s", BucketManager.encodedEntry(bucket, key));
        StringBuilder urlBuilder = new StringBuilder("http://rs.qiniu.com/chtype/")
                .append(UrlSafeBase64.encodeToString(String.join(":", bucket, key)));
        urlBuilder.append("/type/").append(type);
        StringMap headers = auth.authorization(urlBuilder.toString(), null, Client.FormMime);
        Response response = client.post(urlBuilder.toString(), null, headers, Client.FormMime);
        if (response.statusCode != 200) throw new QiniuException(response);
        response.close();
        return String.join("\t", key, "200");
    }

    @Override
    public void closeResource() {
        super.closeResource();
        if (ops != null) ops.clear();
        ops = null;
        if (lines != null) lines.clear();
        lines = null;
        auth = null;
        configuration = null;
        client = null;
    }
}
