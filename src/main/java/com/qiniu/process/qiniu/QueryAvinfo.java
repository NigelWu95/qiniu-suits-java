package com.qiniu.process.qiniu;

import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.Map;

public class QueryAvinfo extends Base<Map<String, String>> {

    private String protocol;
    private String domain;
    private String urlIndex;
    private Configuration configuration;
    private MediaManager mediaManager;

    public QueryAvinfo(Configuration configuration, String protocol, String domain, String urlIndex) throws IOException {
        super("avinfo", "", "", null);
        set(configuration, protocol, domain, urlIndex);
        this.mediaManager = new MediaManager(configuration, protocol);
    }

    public QueryAvinfo(Configuration configuration, String protocol, String domain, String urlIndex, String savePath,
                       int saveIndex) throws IOException {
        super("avinfo", "", "", null, savePath, saveIndex);
        set(configuration, protocol, domain, urlIndex);
        this.mediaManager = new MediaManager(configuration, protocol);
    }

    public QueryAvinfo(Configuration configuration, String protocol, String domain, String urlIndex, String savePath)
            throws IOException {
        this(configuration, protocol, domain, urlIndex, savePath, 0);
    }

    private void set(Configuration configuration, String protocol, String domain, String urlIndex) throws IOException {
        this.configuration = configuration;
        if (domain == null || "".equals(domain)) {
            if (urlIndex == null || "".equals(urlIndex)) {
                throw new IOException("please set one of domain and url-index.");
            } else {
                this.urlIndex = urlIndex;
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
    public QueryAvinfo clone() throws CloneNotSupportedException {
        QueryAvinfo queryAvinfo = (QueryAvinfo)super.clone();
        queryAvinfo.mediaManager = new MediaManager(configuration, protocol);
        return queryAvinfo;
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
            // 解析出文件名是为了便于后续可能需要通过 key 和 avinfo 来进行一些其他对应操作
            return String.join("\t", URLUtils.getKey(url), JsonUtils.toJson(mediaManager.getAvinfoBody(url)), url);
        } else {
            String key = line.get("key");
            if (key == null) throw new IOException("key is not exists or empty in " + line);
            url = String.join("", protocol, "://", domain, "/", key.replace("\\?", "%3f"));
//            line.put(urlIndex, url);
            return String.join("\t", key, JsonUtils.toJson(mediaManager.getAvinfoBody(url)));
        }
    }

    @Override
    public void closeResource() {
        super.closeResource();
        protocol = null;
        domain = null;
        urlIndex = null;
        configuration = null;
        mediaManager = null;
    }
}
