package com.qiniu.process.qdora;

import com.google.gson.JsonParseException;
import com.qiniu.common.QiniuException;
import com.qiniu.process.Base;
import com.qiniu.process.qoss.FileChecker;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.Map;

public class QueryHash extends Base<Map<String, String>> {

    private String algorithm;
    private String domain;
    private String protocol;
    private String urlIndex;
    private FileChecker fileChecker;

    public QueryHash(Configuration configuration, String algorithm, String protocol, String domain, String urlIndex)
            throws IOException {
        super("qhash", "", "", configuration, null);
        set(algorithm, protocol, domain, urlIndex);
        this.fileChecker = new FileChecker(configuration.clone(), algorithm, protocol);
    }

    public QueryHash(Configuration configuration, String algorithm, String protocol, String domain, String urlIndex,
                     String savePath, int saveIndex) throws IOException {
        super("qhash", "", "", configuration, null, savePath, saveIndex);
        set(algorithm, protocol, domain, urlIndex);
        this.fileChecker = new FileChecker(configuration.clone(), algorithm, protocol);
    }

    public QueryHash(Configuration configuration, String algorithm, String protocol, String domain, String urlIndex,
                     String savePath) throws IOException {
        this(configuration, algorithm, protocol, domain, urlIndex, savePath, 0);
    }

    private void set(String algorithm, String protocol, String domain, String urlIndex) throws IOException {
        this.algorithm = algorithm;
        if (urlIndex == null || "".equals(urlIndex)) {
            this.urlIndex = "url";
            if (domain == null || "".equals(domain)) {
                throw new IOException("please set one of domain and url-index.");
            } else {
                RequestUtils.lookUpFirstIpFromHost(domain);
                this.domain = domain;
                this.protocol = protocol == null || !protocol.matches("(http|https)") ? "http" : protocol;
            }
        } else {
            this.urlIndex = urlIndex;
        }
    }

    public void updateQuery(String algorithm, String protocol, String domain, String urlIndex)
            throws IOException {
        set(algorithm, protocol, domain, urlIndex);
        this.fileChecker = new FileChecker(configuration.clone(), algorithm, protocol);
    }

    public QueryHash clone() throws CloneNotSupportedException {
        QueryHash queryHash = (QueryHash)super.clone();
        queryHash.fileChecker = new FileChecker(configuration.clone(), algorithm, protocol);
        return queryHash;
    }

    @Override
    public String resultInfo(Map<String, String> line) {
        return line.get("key") + "\t" + line.get(urlIndex);
    }

    @Override
    public boolean validCheck(Map<String, String> line) {
        return line.get("key") != null;
    }

    @Override
    public String singleResult(Map<String, String> line) throws QiniuException {
        String url =  line.get(urlIndex);
        if (url == null || "".equals(url)) {
            url = protocol + "://" + domain + "/" + line.get("key").replaceAll("\\?", "%3F");
            line.put(urlIndex, url);
        }
        String qhash = fileChecker.getQHashBody(url);
        if (qhash != null && !"".equals(qhash)) {
            // 由于响应的 body 为多行需经过格式化处理为一行字符串
            try {
                return JsonUtils.toJson(qhash);
            } catch (JsonParseException e) {
                throw new QiniuException(e, e.getMessage());
            }
        } else {
            throw new QiniuException(null, "empty_result");
        }
    }
}
