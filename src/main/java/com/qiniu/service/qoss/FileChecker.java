package com.qiniu.service.qoss;

import com.google.gson.*;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.model.qoss.Qhash;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.JsonConvertUtils;
import com.qiniu.util.RequestUtils;

import java.net.UnknownHostException;

public class FileChecker {

    private Client client;
    private String algorithm;

    public FileChecker(String algorithm) {
        this.client = new Client();
        this.algorithm = (algorithm == null || "".equals(algorithm)) ? "md5" : algorithm;
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

        return getQHashByJson(getQHashJson(domain, sourceKey)) ;
    }

    /**
     *
     * @param domain
     * @param sourceKey
     * @return
     * @throws QiniuException
     */
    public JsonObject getQHashJson(String domain, String sourceKey) throws QiniuException {

        try {
            RequestUtils.checkHost(domain);
        } catch (UnknownHostException e) {
            throw new QiniuException(e);
        }
        String url = "http://" + domain + "/" + sourceKey.split("\\?")[0];
        Response response = client.get(url + "?qhash/" + algorithm);
        JsonObject qhashJson = JsonConvertUtils.toJsonObject(response.bodyString());
        response.close();
        return qhashJson;
    }

    public Qhash getQHashByJson(String qHashJson) throws QiniuException {

        return getQHashByJson(JsonConvertUtils.toJsonObject(qHashJson));
    }

    public Qhash getQHashByJson(JsonObject qHashJson) throws QiniuException {

        Qhash qhash;
        try {
            qhash = JsonConvertUtils.fromJson(qHashJson, Qhash.class);
        } catch (JsonParseException e) {
            throw new QiniuException(e, e.getMessage());
        }
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

        return getStatByJson(getStatJson(domain, sourceKey)) ;
    }

    /**
     *
     * @param domain
     * @param sourceKey
     * @return
     * @throws QiniuException
     */
    public JsonObject getStatJson(String domain, String sourceKey) throws QiniuException {

        try {
            RequestUtils.checkHost(domain);
        } catch (UnknownHostException e) {
            throw new QiniuException(e);
        }
        String url = "http://" + domain + "/" + sourceKey.split("\\?")[0];
        Response response = client.get(url + "?stat");
        JsonObject fileInfoJson = JsonConvertUtils.toJsonObject(response.bodyString());
        response.close();
        return fileInfoJson;
    }

    public FileInfo getStatByJson(String fileInfoJson) throws QiniuException {

        return getStatByJson(JsonConvertUtils.toJsonObject(fileInfoJson));
    }

    public FileInfo getStatByJson(JsonObject fileInfoJson) throws QiniuException {

        FileInfo fileInfo;
        try {
            fileInfo = JsonConvertUtils.fromJson(fileInfoJson, FileInfo.class);
        } catch (JsonParseException e) {
            throw new QiniuException(e, e.getMessage());
        }
        return fileInfo;
    }
}
