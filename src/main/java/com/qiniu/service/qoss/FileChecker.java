package com.qiniu.service.qoss;

import com.google.gson.*;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.model.qoss.Qhash;
import com.qiniu.util.Auth;

public class FileChecker {

    final private Client client;
    final private String algorithm;
    final private String protocol;
    final private Auth srcAuth;

    public FileChecker() {
        this.client = new Client();
        this.algorithm = "md5";
        this.protocol = "http";
        this.srcAuth = null;
    }

    public FileChecker(String algorithm, String protocol, Auth srcAuth) {
        this.client = new Client();
        this.algorithm = (algorithm == null || "".equals(algorithm)) ? "md5" : algorithm;
        this.protocol = protocol == null || !protocol.matches("(http|https)") ? "http" : protocol;
        this.srcAuth = srcAuth;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public Qhash getQHash(String url) throws QiniuException {
        return getQHashByJson(getQHashBody(url));
    }

    public Qhash getQHash(String domain, String sourceKey) throws QiniuException {
        return getQHashByJson(getQHashBody(domain, sourceKey));
    }

    public Qhash getQHashByJson(String qHashJson) throws QiniuException {
        Qhash qhash;
        try {
            Gson gson = new Gson();
            qhash = gson.fromJson(qHashJson, Qhash.class);
        } catch (JsonParseException e) {
            throw new QiniuException(e, e.getMessage());
        }
        return qhash;
    }

    public Qhash getQHashByJson(JsonObject qHashJson) throws QiniuException {
        Qhash qhash;
        try {
            Gson gson = new Gson();
            qhash = gson.fromJson(qHashJson, Qhash.class);
        } catch (JsonParseException e) {
            throw new QiniuException(e, e.getMessage());
        }
        return qhash;
    }

    public JsonObject getQHashJson(String domain, String sourceKey) throws QiniuException {
        JsonParser jsonParser = new JsonParser();
        return jsonParser.parse(getQHashBody(domain, sourceKey)).getAsJsonObject();
    }

    public String getQHashBody(String domain, String sourceKey) throws QiniuException {
        String url = protocol + "://" + domain + "/" + sourceKey.split("\\?")[0];
        return getQHashBody(url);
    }

    public String getQHashBody(String url) throws QiniuException {
        url = srcAuth != null ? srcAuth.privateDownloadUrl(url + "?qhash/" + algorithm) : url + "?qhash/" + algorithm;
        Response response = client.get(url);
        String qhash = response.bodyString();
        response.close();
        return qhash;
    }
}
