package com.qiniu.service.qoss;

import com.google.gson.*;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.model.qoss.Qhash;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.RequestUtils;

import java.net.UnknownHostException;

public class FileChecker {

    private Client client;
    private String algorithm;
    private boolean https;
    private Auth srcAuth;

    public FileChecker(String algorithm, boolean https, Auth srcAuth) {
        this.client = new Client();
        this.algorithm = (algorithm == null || "".equals(algorithm)) ? "md5" : algorithm;
        this.https = https;
        this.srcAuth = srcAuth;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public Qhash getQHash(String url) throws QiniuException, UnknownHostException {

        String[] addr = url.split("/");
        if (addr.length < 3) throw new QiniuException(null, "not valid url.");
        String domain = addr[2];
        RequestUtils.checkHost(domain);
        StringBuilder key = new StringBuilder();
        for (int i = 3; i < addr.length; i++) {
            key.append(addr[i]).append("/");
        }
        return getQHash(domain, key.toString().substring(0, key.length() - 1));
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

        try {
            RequestUtils.checkHost(domain);
        } catch (UnknownHostException e) {
            throw new QiniuException(e);
        }
        String url = (https ? "https://" : "http://") + domain + "/" + sourceKey.split("\\?")[0];
        url = srcAuth != null ? srcAuth.privateDownloadUrl(url + "?qhash/" + algorithm) : url + "?qhash/" + algorithm;
        Response response = client.get(url);
        String qhash = response.bodyString();
        response.close();
        return qhash;
    }

    public FileInfo getStat(String url) throws QiniuException, UnknownHostException {

        String[] addr = url.split("/");
        if (addr.length < 3) throw new QiniuException(null, "not valid url.");
        String domain = addr[2];
        RequestUtils.checkHost(domain);
        StringBuilder key = new StringBuilder();
        for (int i = 3; i < addr.length; i++) {
            key.append(addr[i]).append("/");
        }
        return getStat(domain, key.toString().substring(0, key.length() - 1));
    }

    public FileInfo getStat(String domain, String sourceKey) throws QiniuException {

        return getStatByJson(getStatBody(domain, sourceKey)) ;
    }

    public FileInfo getStatByJson(String fileInfoJson) throws QiniuException {

        FileInfo stat;
        try {
            Gson gson = new Gson();
            stat = gson.fromJson(fileInfoJson, FileInfo.class);
        } catch (JsonParseException e) {
            throw new QiniuException(e, e.getMessage());
        }
        return stat;
    }

    public FileInfo getStatByJson(JsonObject fileInfoJson) throws QiniuException {

        FileInfo stat;
        try {
            Gson gson = new Gson();
            stat = gson.fromJson(fileInfoJson, FileInfo.class);
        } catch (JsonParseException e) {
            throw new QiniuException(e, e.getMessage());
        }
        return stat;
    }

    public JsonObject getStatJson(String domain, String sourceKey) throws QiniuException {

        JsonParser jsonParser = new JsonParser();
        return jsonParser.parse(getStatBody(domain, sourceKey)).getAsJsonObject();
    }

    public String getStatBody(String domain, String sourceKey) throws QiniuException {

        try {
            RequestUtils.checkHost(domain);
        } catch (UnknownHostException e) {
            throw new QiniuException(e);
        }
        String url = (https ? "https://" : "http://") + domain + "/" + sourceKey.split("\\?")[0];
        url = srcAuth != null ? srcAuth.privateDownloadUrl(url + "?stat") : url + "?stat";
        Response response = client.get(url);
        String stat = response.bodyString();
        response.close();
        return stat;
    }
}
