package com.qiniu.process.qiniu;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RestoreArchive extends Base<Map<String, String>> {

    private int days;
    private String condition;
    private Configuration configuration;
    private Auth auth;
    private Client client;
    private static String requestUrl;

    public RestoreArchive(String accessKey, String secretKey, Configuration configuration, String bucket, int days,
                          String condition) throws IOException {
        super("restorear", accessKey, secretKey, bucket);
        this.days = days;
        this.condition = condition;
        this.configuration = configuration;
        this.auth = Auth.create(accessKey, secretKey);
        this.client = new Client(configuration.clone());
        CloudApiUtils.checkQiniu(accessKey, secretKey, configuration, bucket);
        requestUrl = configuration.useHttpsDomains ? "https://rs.qbox.me/restoreAr" : "http://rs.qbox.me/restoreAr";
    }

    public RestoreArchive(String accessKey, String secretKey, Configuration configuration, String bucket, int days,
                          String condition, String savePath, int saveIndex) throws IOException {
        super("restorear", accessKey, secretKey, bucket, savePath, saveIndex);
        this.days = days;
        this.condition = condition;
        this.configuration = configuration;
        this.auth = Auth.create(accessKey, secretKey);
        this.client = new Client(configuration.clone());
        CloudApiUtils.checkQiniu(accessKey, secretKey, configuration, bucket);
        requestUrl = configuration.useHttpsDomains ? "https://rs.qbox.me/restoreAr" : "http://rs.qbox.me/restoreAr";
    }

    public RestoreArchive(String accessKey, String secretKey, Configuration configuration, String bucket, int days,
                          String condition, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, days, condition, savePath, 0);
    }

    @Override
    public RestoreArchive clone() throws CloneNotSupportedException {
        RestoreArchive restoreArchive = (RestoreArchive)super.clone();
        restoreArchive.auth = Auth.create(accessId, secretKey);
        restoreArchive.client = new Client(configuration.clone());
        return restoreArchive;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get("key");
    }

    @Override
    protected String batchResult(List<Map<String, String>> lineList) throws IOException {
        JsonObject bodyJson = new JsonObject();
        JsonArray entries = new JsonArray();
        JsonObject entry = new JsonObject();
        entry.addProperty("bucket", bucket);
        for (Map<String, String> line : lineList) {
            entry.addProperty("key", line.get("key"));
            entry.addProperty("freeze_after_days", days);
            entry.addProperty("cond", condition);
            entries.add(entry);
        }
        bodyJson.add("entries", entries);
        byte[] body = bodyJson.toString().getBytes();
        bodyJson = null;
        entries = null;
        entry = null;
        return HttpRespUtils.getResult(client.post(requestUrl, body,
                auth.authorizationV2(requestUrl, "POST", body, Client.JsonMime), Client.JsonMime));
    }

    @Override
    protected List<Map<String, String>> parseBatchResult(List<Map<String, String>> processList, String result) throws Exception {
        // 归档存储解冻操作本身不响应任何 body 内容
//        if (result == null || "".equals(result)) throw new IOException("not valid refresh response.");
        if ("".equals(result)) {
            fileSaveMapper.writeSuccess(processList.stream().map(this::resultInfo).collect(Collectors.joining("\n")), false);
            return null;
        } else {
            return super.parseBatchResult(processList, result);
        }
    }

    @Override
    protected String singleResult(Map<String, String> line) throws Exception {
        JsonObject bodyJson = new JsonObject();
        JsonArray entries = new JsonArray();
        JsonObject entry = new JsonObject();
        entry.addProperty("bucket", bucket);
        String key = line.get("key");
        entry.addProperty("key", key);
        entry.addProperty("freeze_after_days", days);
        entry.addProperty("cond", condition);
        entries.add(entry);
        bodyJson.add("entries", entries);
        byte[] body = bodyJson.toString().getBytes();
        Response response = client.post(requestUrl, body,
                auth.authorizationV2(requestUrl, "POST", body, Client.JsonMime), Client.JsonMime);
        if (response.statusCode != 200) throw new QiniuException(response);
        response.close();
        bodyJson = null;
        entries = null;
        entry = null;
        body = null;
        return String.join("\t", key, "200");
    }

    @Override
    public void closeResource() {
        super.closeResource();
        condition = null;
        configuration = null;
        auth = null;
        client = null;
    }
}
