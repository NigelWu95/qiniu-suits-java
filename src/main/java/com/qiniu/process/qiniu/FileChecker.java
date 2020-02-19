package com.qiniu.process.qiniu;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.model.qos.Qhash;
import com.qiniu.storage.Configuration;
import com.qiniu.util.JsonUtils;

import java.util.ArrayList;
import java.util.List;

public class FileChecker {

    private Client client;
    private String protocol;
    private String algorithm;
    final private static List<String> algorithms = new ArrayList<String>(){{
        add("md5");
        add("sha1");
    }};

    public FileChecker() {
        this.protocol = "http";
        this.algorithm = "md5";
    }

    public FileChecker(String protocol, String algorithm) {
        this.client = new Client();
        this.protocol = "https".equals(protocol)? "https" : "http";
        this.algorithm = algorithms.contains(algorithm) ? algorithm : "md5";
    }

    public FileChecker(Configuration configuration, String protocol, String algorithm) {
        this.client = configuration == null ? new Client() : new Client(configuration.clone());
        this.protocol = "https".equals(protocol)? "https" : "http";
        this.algorithm = algorithms.contains(algorithm) ? algorithm : "md5";
    }

    public Qhash getQHash(String url) throws QiniuException {
        return getQHashByJson(getQHashBody(url));
    }

    public Qhash getQHash(String domain, String sourceKey) throws QiniuException {
        return getQHashByJson(getQHashBody(domain, sourceKey));
    }

    public Qhash getQHashByJson(String qHashJson) {
        return JsonUtils.fromJson(qHashJson, Qhash.class);
    }

    public String getQHashBody(String domain, String sourceKey) throws QiniuException {
        String url = String.join("", protocol, "://", domain, "/", sourceKey.split("\\?")[0]);
        return getQHashBody(url);
    }

    public String getQHashBody(String url) throws QiniuException {
        if (client == null) this.client = new Client();
        Response response = client.get(String.join("", url, "?qhash/", algorithm));
        String qhash = response.bodyString();
        if (response.statusCode != 200 || qhash.isEmpty()) throw new QiniuException(response);
        response.close();
        return qhash;
    }
}
