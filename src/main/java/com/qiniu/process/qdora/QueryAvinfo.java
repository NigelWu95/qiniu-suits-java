package com.qiniu.process.qdora;

import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.Map;

public class QueryAvinfo extends Base<Map<String, String>> {

    private String domain;
    private String protocol;
    private String urlIndex;
    private Configuration configuration;
    private MediaManager mediaManager;

    public QueryAvinfo(Configuration configuration, String domain, String protocol, String urlIndex) throws IOException {
        super("avinfo", "", "", null);
        set(configuration, protocol, domain, urlIndex);
        this.mediaManager = new MediaManager(configuration.clone(), protocol);
    }

    public QueryAvinfo(Configuration configuration, String domain, String protocol, String urlIndex, String savePath,
                       int saveIndex) throws IOException {
        super("avinfo", "", "", null, savePath, saveIndex);
        set(configuration, protocol, domain, urlIndex);
        this.mediaManager = new MediaManager(configuration.clone(), protocol);
    }

    public QueryAvinfo(Configuration configuration, String domain, String protocol, String urlIndex, String savePath)
            throws IOException {
        this(configuration, domain, protocol, urlIndex, savePath, 0);
    }

    private void set(Configuration configuration, String protocol, String domain, String urlIndex) throws IOException {
        this.configuration = configuration;
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

    public QueryAvinfo clone() throws CloneNotSupportedException {
        QueryAvinfo queryAvinfo = (QueryAvinfo)super.clone();
        queryAvinfo.mediaManager = new MediaManager(configuration.clone(), protocol);
        return queryAvinfo;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        String key = line.get("key");
        return (key == null ? "\t" : key + "\t") + line.get(urlIndex);
    }

    protected String singleResult(Map<String, String> line) throws Exception {
        String url = line.get(urlIndex);
        String key = line.get("key");
        if (url == null || "".equals(url)) {
            if (key == null) throw new IOException("key is not exists or empty in " + line);
            url = protocol + "://" + domain + "/" + key.replace("\\?", "%3f");
            line.put(urlIndex, url);
            return key + "\t" + url + "\t" + JsonUtils.toJson(mediaManager.getAvinfoBody(url));
        }
        return (key == null ? "\t" : key + "\t") + url + "\t" + JsonUtils.toJson(mediaManager.getAvinfoBody(url));
    }

    @Override
    public void closeResource() {
        super.closeResource();
        configuration = null;
        mediaManager = null;
        domain = null;
        protocol = null;
        urlIndex = null;
    }
}
