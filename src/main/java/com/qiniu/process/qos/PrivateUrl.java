package com.qiniu.process.qos;

import com.qiniu.interfaces.ILineProcess;
import com.qiniu.process.Base;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.Map;

public class PrivateUrl extends Base<Map<String, String>> {

    private Auth auth;
    private String domain;
    private String protocol;
    private String urlIndex;
    private String suffixOrQuery;
    private boolean useQuery;
    private long expires;
    private ILineProcess<Map<String, String>> nextProcessor;

    public PrivateUrl(String accessKey, String secretKey, String domain, String protocol, String urlIndex, String suffixOrQuery,
                      long expires) throws IOException {
        super("privateurl", accessKey, secretKey, null);
        this.auth = Auth.create(accessKey, secretKey);
        CloudAPIUtils.checkQiniu(auth);
        set(domain, protocol, urlIndex, suffixOrQuery, expires);
    }

    public PrivateUrl(String accessKey, String secretKey, String domain, String protocol, String urlIndex, String suffixOrQuery,
                      long expires, String savePath, int saveIndex) throws IOException {
        super("privateurl", accessKey, secretKey, null, savePath, saveIndex);
        this.auth = Auth.create(accessKey, secretKey);
        CloudAPIUtils.checkQiniu(auth);
        set(domain, protocol, urlIndex, suffixOrQuery, expires);
    }

    public PrivateUrl(String accessKey, String secretKey, String domain, String protocol, String urlIndex, String suffixOrQuery,
                      long expires, String savePath) throws IOException {
        this(accessKey, secretKey, domain, protocol, urlIndex, suffixOrQuery, expires, savePath, 0);
    }

    private void set(String domain, String protocol, String urlIndex, String suffixOrQuery, long expires) throws IOException {
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
        this.suffixOrQuery = suffixOrQuery == null ? "" : suffixOrQuery;
        useQuery = !"".equals(this.suffixOrQuery);
        this.expires = expires <= 0L ? 3600 : expires;
    }

    public void setNextProcessor(ILineProcess<Map<String, String>> nextProcessor) {
        this.nextProcessor = nextProcessor;
        if (nextProcessor != null) processName = nextProcessor.getProcessName() + "_with_" + processName;
    }

    public PrivateUrl clone() throws CloneNotSupportedException {
        PrivateUrl privateUrl = (PrivateUrl)super.clone();
        privateUrl.auth = Auth.create(authKey1, authKey2);
        if (nextProcessor != null) privateUrl.nextProcessor = nextProcessor.clone();
        return privateUrl;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        String key = line.get("key");
        return (key == null ? "\t" : key + "\t") + line.get(urlIndex);
    }

    @Override
    protected String singleResult(Map<String, String> line) throws Exception {
        String url = line.get(urlIndex);
        String key = line.get("key");
        if (url == null || "".equals(url)) {
            if (key == null) throw new IOException("key is not exists or empty in " + line);
            url = protocol + "://" + domain + "/" + key.replaceAll("\\?", "%3f") + suffixOrQuery;
            line.put(urlIndex, url);
        } else if (useQuery) {
            url = url + suffixOrQuery;
            line.put(urlIndex, url);
        }
        url = auth.privateDownloadUrl(url, expires);
        if (nextProcessor != null) {
            line.put("url", url);
            return nextProcessor.processLine(line);
        }
        return (key == null ? "\t" : key + "\t") + url;
    }

    @Override
    public void closeResource() {
        super.closeResource();
        auth = null;
        domain = null;
        protocol = null;
        urlIndex = null;
        if (nextProcessor != null) nextProcessor.closeResource();
        nextProcessor = null;
    }
}
