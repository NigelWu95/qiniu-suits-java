package com.qiniu.process.other;

import com.qiniu.interfaces.ILineProcess;
import com.qiniu.process.Base;
import com.qiniu.util.RequestUtils;

import java.io.IOException;
import java.util.Map;

public class PublicUrl extends Base<Map<String, String>> {

    private String protocol;
    private String domain;
    private String urlIndex;
    private String suffixOrQuery;
    private ILineProcess<Map<String, String>> nextProcessor;

    public PublicUrl(String accessKey, String secretKey, String protocol, String domain, String urlIndex, String suffixOrQuery)
            throws IOException {
        super("publicurl", accessKey, secretKey, null);
        set(protocol, domain, urlIndex, suffixOrQuery);
    }

    public PublicUrl(String accessKey, String secretKey, String protocol, String domain, String urlIndex, String suffixOrQuery,
                     String savePath, int saveIndex) throws IOException {
        super("publicurl", accessKey, secretKey, null, savePath, saveIndex);
        set(protocol, domain, urlIndex, suffixOrQuery);
    }

    public PublicUrl(String accessKey, String secretKey, String protocol, String domain, String urlIndex, String suffixOrQuery,
                     String savePath) throws IOException {
        this(accessKey, secretKey, protocol, domain, urlIndex, suffixOrQuery, savePath, 0);
    }

    private void set(String protocol, String domain, String urlIndex, String suffixOrQuery) throws IOException {
        this.suffixOrQuery = suffixOrQuery == null ? "" : suffixOrQuery;
        if (domain == null || "".equals(domain)) {
            if (urlIndex == null || "".equals(urlIndex)) {
                throw new IOException("please set one of domain and url-index.");
            } else {
                this.urlIndex = urlIndex;
                if ("".equals(this.suffixOrQuery)) throw new IOException("please set suffix or query if url-index used.");
            }
            this.domain = null;
        } else {
            this.protocol = protocol == null || !protocol.matches("(http|https)") ? "http" : protocol;
            RequestUtils.lookUpFirstIpFromHost(domain);
            this.domain = domain;
            this.urlIndex = "url";
        }
    }

    @Override
    public void setNextProcessor(ILineProcess<Map<String, String>> nextProcessor) {
        this.nextProcessor = nextProcessor;
        if (nextProcessor != null) processName = String.join("_with_", nextProcessor.getProcessName(), processName);
    }

    @Override
    public PublicUrl clone() throws CloneNotSupportedException {
        PublicUrl privateUrl = (PublicUrl)super.clone();
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
            if (nextProcessor == null) return url + suffixOrQuery;
        } else {
            String key = line.get("key");
            if (key == null) throw new IOException("key is not exists or empty in " + line);
            url = String.join("", protocol, "://", domain, "/", key.replace("\\?", "%3f"), suffixOrQuery);
            if (nextProcessor == null) return url;
        }
        line.put("url", url);
        return nextProcessor.processLine(line);
    }

    @Override
    public void closeResource() {
        super.closeResource();
        protocol = null;
        domain = null;
        suffixOrQuery = null;
        if (nextProcessor != null) nextProcessor.closeResource();
        nextProcessor = null;
    }
}
