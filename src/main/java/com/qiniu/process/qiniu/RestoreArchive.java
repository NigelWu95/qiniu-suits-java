package com.qiniu.process.qiniu;

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
import java.util.stream.Collectors;

public class RestoreArchive extends Base<Map<String, String>> {

    private int days;
    private ArrayList<String> ops;
    private List<Map<String, String>> lines;
    private Configuration configuration;
    private Auth auth;
    private Client client;
    private static String requestUrl;

    public RestoreArchive(String accessKey, String secretKey, Configuration configuration, String bucket, int days)
            throws IOException {
        super("restorear", accessKey, secretKey, bucket);
        this.days = days;
        this.configuration = configuration;
        this.auth = Auth.create(accessKey, secretKey);
        this.client = new Client(configuration.clone());
        CloudApiUtils.checkQiniu(accessKey, secretKey, configuration, bucket);
        requestUrl = configuration.useHttpsDomains ? "https://rs.qbox.me/restoreAr/" : "http://rs.qbox.me/restoreAr/";
    }

    public RestoreArchive(String accessKey, String secretKey, Configuration configuration, String bucket, int days,
                          String savePath, int saveIndex) throws IOException {
        super("restorear", accessKey, secretKey, bucket, savePath, saveIndex);
        this.days = days;
        this.batchSize = 1000;
        this.ops = new ArrayList<>(1000);
        this.lines = new ArrayList<>(1000);
        this.configuration = configuration;
        this.auth = Auth.create(accessKey, secretKey);
        this.client = new Client(configuration.clone());
        CloudApiUtils.checkQiniu(accessKey, secretKey, configuration, bucket);
        requestUrl = configuration.useHttpsDomains ? "https://rs.qbox.me/restoreAr/" : "http://rs.qbox.me/restoreAr/";
    }

    public RestoreArchive(String accessKey, String secretKey, Configuration configuration, String bucket, int days,
                          String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, days, savePath, 0);
    }

    @Override
    public RestoreArchive clone() throws CloneNotSupportedException {
        RestoreArchive restoreArchive = (RestoreArchive)super.clone();
        if (fileSaveMapper != null) {
            restoreArchive.ops = new ArrayList<>(batchSize);
            restoreArchive.lines = new ArrayList<>(batchSize);
        }
        restoreArchive.auth = Auth.create(accessId, secretKey);
        restoreArchive.client = new Client(configuration.clone());
        return restoreArchive;
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
//        String encodedMetaValue;
//        String path;
        StringBuilder pathBuilder;
        for (Map<String, String> map : processList) {
            key = map.get("key");
            if (key != null) {
                lines.add(map);
                pathBuilder = new StringBuilder("/restoreAr/")
                        .append(UrlSafeBase64.encodeToString(String.join(":", bucket, key)))
                        .append("/freezeAfterDays/").append(days);
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
    protected List<Map<String, String>> parseBatchResult(List<Map<String, String>> processList, String result) throws Exception {
        // 归档存储解冻操作可能不响应任何 body 内容
//        if (result == null || "".equals(result)) throw new IOException("not valid refresh response.");
        if ("".equals(result)) {
            fileSaveMapper.writeSuccess(processList.stream().map(this::resultInfo).collect(Collectors.joining("\n")), false);
            return null;
        } else {
            return super.parseBatchResult(processList, result);
        }
    }

    @Override
    protected String singleResult(Map<String, String> line) throws IOException {
        String key = line.get("key");
        if (key == null) throw new IOException("key is not exists or empty in " + line);
        StringBuilder urlBuilder = new StringBuilder(requestUrl)
                .append(UrlSafeBase64.encodeToString(String.join(":", bucket, key)))
                .append("/freezeAfterDays/").append(days);
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
        configuration = null;
        auth = null;
        client = null;
    }
}
