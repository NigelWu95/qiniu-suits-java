package com.qiniu.process.qiniu;

import com.qiniu.interfaces.ILineProcess;
import com.qiniu.process.Base;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.Map;

public class PrivateUrl extends Base<Map<String, String>> {

    private Auth auth;
    private String protocol;
    private String domain;
    private String urlIndex;
    private String suffixOrQuery;
    private long expires;
    private ILineProcess<Map<String, String>> nextProcessor;

    public PrivateUrl(String accessKey, String secretKey, String protocol, String domain, String urlIndex, String suffixOrQuery,
                      long expires) throws IOException {
        super("privateurl", accessKey, secretKey, null);
        this.auth = Auth.create(accessKey, secretKey);
        CloudApiUtils.checkQiniu(auth);
        set(protocol, domain, urlIndex, suffixOrQuery, expires);
    }

    public PrivateUrl(String accessKey, String secretKey, String protocol, String domain, String urlIndex, String suffixOrQuery,
                      long expires, String savePath, int saveIndex) throws IOException {
        super("privateurl", accessKey, secretKey, null, savePath, saveIndex);
        this.auth = Auth.create(accessKey, secretKey);
        CloudApiUtils.checkQiniu(auth);
        set(protocol, domain, urlIndex, suffixOrQuery, expires);
    }

    public PrivateUrl(String accessKey, String secretKey, String protocol, String domain, String urlIndex, String suffixOrQuery,
                      long expires, String savePath) throws IOException {
        this(accessKey, secretKey, protocol, domain, urlIndex, suffixOrQuery, expires, savePath, 0);
    }

    private void set(String protocol, String domain, String urlIndex, String suffixOrQuery, long expires) throws IOException {
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
        this.suffixOrQuery = suffixOrQuery == null ? "" : suffixOrQuery;
        this.expires = expires <= 0L ? 3600 : expires;
    }

    @Override
    public void setNextProcessor(ILineProcess<Map<String, String>> nextProcessor) {
        this.nextProcessor = nextProcessor;
        if (nextProcessor != null) processName = String.join("_with_", nextProcessor.getProcessName(), processName);
    }

    @Override
    public PrivateUrl clone() throws CloneNotSupportedException {
        PrivateUrl privateUrl = (PrivateUrl)super.clone();
        privateUrl.auth = Auth.create(accessId, secretKey);
        if (nextProcessor != null) privateUrl.nextProcessor = nextProcessor.clone();
        return privateUrl;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return domain == null ? line.get(urlIndex) : line.get("key");
    }

    @Override
    protected String singleResult(Map<String, String> line) throws Exception {
        String url;
        if (domain == null) {
            url = line.get(urlIndex);
            if (nextProcessor == null) return String.join("\t", auth
                    .privateDownloadUrl(url + suffixOrQuery, expires));
        } else {
            String key = line.get("key");
            if (key == null) throw new IOException("key is not exists or empty in " + line);
            url = String.join("", protocol, "://", domain, "/",
                    key.replace("\\?", "%3f"), suffixOrQuery);
            if (nextProcessor == null) return String.join("\t", auth.privateDownloadUrl(url, expires));
        }
        url = auth.privateDownloadUrl(url, expires);
        line.put("url", auth.privateDownloadUrl(url, expires));
        return nextProcessor.processLine(line);
    }

    @Override
    public void closeResource() {
        super.closeResource();
        auth = null;
        protocol = null;
        domain = null;
        urlIndex = null;
        suffixOrQuery = null;
        if (nextProcessor != null) nextProcessor.closeResource();
        nextProcessor = null;
    }
}
