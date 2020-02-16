package com.qiniu.process.other;

import com.qiniu.interfaces.ILineProcess;
import com.qiniu.process.Base;
import com.qiniu.util.RequestUtils;

import java.io.IOException;
import java.util.Map;

public class PublicUrl extends Base<Map<String, String>> {

    private String protocol;
    private String domain;
    private String suffixOrQuery;
    private boolean useQuery;
    private ILineProcess<Map<String, String>> nextProcessor;

    public PublicUrl(String accessKey, String secretKey, String protocol, String domain, String suffixOrQuery) throws IOException {
        super("privateurl", accessKey, secretKey, null);
        set(protocol, domain, suffixOrQuery);
    }

    public PublicUrl(String accessKey, String secretKey, String protocol, String domain, String suffixOrQuery, String savePath,
                     int saveIndex) throws IOException {
        super("privateurl", accessKey, secretKey, null, savePath, saveIndex);
        set(protocol, domain, suffixOrQuery);
    }

    public PublicUrl(String accessKey, String secretKey, String protocol, String domain, String suffixOrQuery, String savePath)
            throws IOException {
        this(accessKey, secretKey, protocol, domain, suffixOrQuery, savePath, 0);
    }

    private void set(String protocol, String domain, String suffixOrQuery) throws IOException {
        if (domain == null || "".equals(domain)) {
            throw new IOException("please set one of domain and url-index.");
        } else {
            this.protocol = protocol == null || !protocol.matches("(http|https)") ? "http" : protocol;
            RequestUtils.lookUpFirstIpFromHost(domain);
            this.domain = domain;
        }
        this.suffixOrQuery = suffixOrQuery == null ? "" : suffixOrQuery;
        useQuery = !"".equals(this.suffixOrQuery);
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
        return line.get("key");
    }

    @Override
    protected String singleResult(Map<String, String> line) throws Exception {
        String key = line.get("key");
        if (key == null) throw new IOException("key is not exists or empty in " + line);
        String url = String.join("", protocol, "://", domain, "/",
                key.replace("\\?", "%3f"), suffixOrQuery);
        if (nextProcessor != null) {
            line.put("url", url);
            return nextProcessor.processLine(line);
        }
        return url;
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
