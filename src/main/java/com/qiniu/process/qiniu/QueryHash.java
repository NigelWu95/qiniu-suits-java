package com.qiniu.process.qiniu;

import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.Map;

public class QueryHash extends Base<Map<String, String>> {

    private String protocol;
    private String domain;
    private String urlIndex;
    private String algorithm;
    private Configuration configuration;
    private FileChecker fileChecker;

    public QueryHash(Configuration configuration, String protocol, String domain, String urlIndex, String algorithm)
            throws IOException {
        super("qhash", "", "", null);
        set(configuration, protocol, domain, urlIndex, algorithm);
        this.fileChecker = new FileChecker(configuration.clone(), protocol, algorithm);
    }

    public QueryHash(Configuration configuration, String protocol, String domain, String urlIndex, String algorithm,
                     String savePath, int saveIndex) throws IOException {
        super("qhash", "", "", null, savePath, saveIndex);
        set(configuration, protocol, domain, urlIndex, algorithm);
        this.fileChecker = new FileChecker(configuration.clone(), protocol, algorithm);
    }

    public QueryHash(Configuration configuration, String protocol, String domain, String urlIndex, String algorithm,
                     String savePath) throws IOException {
        this(configuration, protocol, domain, urlIndex, algorithm, savePath, 0);
    }

    private void set(Configuration configuration, String protocol, String domain, String urlIndex, String algorithm)
            throws IOException {
        this.configuration = configuration;
        if (domain == null || "".equals(domain)) {
            if (urlIndex == null || "".equals(urlIndex)) {
                throw new IOException("please set one of domain and url-index.");
            } else {
                this.urlIndex = urlIndex;
            }
        } else {
            this.protocol = protocol == null || !protocol.matches("(http|https)") ? "http" : protocol;
            RequestUtils.lookUpFirstIpFromHost(domain);
            this.domain = domain;
            this.urlIndex = "url";
        }
        this.algorithm = algorithm;
    }

    @Override
    public QueryHash clone() throws CloneNotSupportedException {
        QueryHash queryHash = (QueryHash)super.clone();
        queryHash.fileChecker = new FileChecker(configuration.clone(), algorithm, protocol);
        return queryHash;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        String key = line.get("key");
        return key == null ? line.get(urlIndex) : String.join("\t", key, line.get(urlIndex));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws Exception {
        String url = line.get(urlIndex);
        String key = line.get("key");
        if (url == null || "".equals(url)) {
            if (key == null) throw new IOException("key is not exists or empty in " + line);
            url = String.join("", protocol, "://", domain, "/", key.replace("\\?", "%3f"));
            line.put(urlIndex, url);
            return String.join("\t", key, url, JsonUtils.toJson(fileChecker.getQHashBody(url)));
        }
        return key == null ? String.join("\t", url, JsonUtils.toJson(fileChecker.getQHashBody(url))) :
                String.join("\t", key, url, JsonUtils.toJson(fileChecker.getQHashBody(url)));
    }

    @Override
    public void closeResource() {
        super.closeResource();
        protocol = null;
        domain = null;
        urlIndex = null;
        algorithm = null;
        configuration = null;
        fileChecker = null;
    }
}
