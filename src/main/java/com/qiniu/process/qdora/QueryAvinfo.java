package com.qiniu.process.qdora;

import com.google.gson.JsonParseException;
import com.qiniu.common.QiniuException;
import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.Map;

public class QueryAvinfo extends Base<Map<String, String>> {

    private String domain;
    private String protocol;
    private String urlIndex;
    private MediaManager mediaManager;

    public QueryAvinfo(Configuration configuration, String domain, String protocol, String urlIndex) throws IOException {
        super("avinfo", "", "", configuration, null);
        set(protocol, domain, urlIndex);
        this.mediaManager = new MediaManager(configuration.clone(), protocol);
    }

    public QueryAvinfo(Configuration configuration, String domain, String protocol, String urlIndex, String savePath,
                       int saveIndex) throws IOException {
        super("avinfo", "", "", configuration, null, savePath, saveIndex);
        set(protocol, domain, urlIndex);
        this.mediaManager = new MediaManager(configuration.clone(), protocol);
    }

    public QueryAvinfo(Configuration configuration, String domain, String protocol, String urlIndex, String savePath)
            throws IOException {
        this(configuration, domain, protocol, urlIndex, savePath, 0);
    }

    private void set(String protocol, String domain, String urlIndex) throws IOException {
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

    public void updateQuery(String protocol, String domain, String urlIndex) throws IOException {
        set(protocol, domain, urlIndex);
        this.mediaManager = new MediaManager(configuration.clone(), protocol);
    }

    public QueryAvinfo clone() throws CloneNotSupportedException {
        QueryAvinfo queryAvinfo = (QueryAvinfo)super.clone();
        queryAvinfo.mediaManager = new MediaManager(configuration.clone(), protocol);
        return queryAvinfo;
    }

    @Override
    public String resultInfo(Map<String, String> line) {
        return line.get("key") + "\t" + line.get(urlIndex);
    }

    @Override
    public boolean validCheck(Map<String, String> line) {
        return line.get("key") != null;
    }

    public String singleResult(Map<String, String> line) throws QiniuException {
        String url = line.get(urlIndex);
        if (url == null || "".equals(url)) {
            url = protocol + "://" + domain + "/" + line.get("key").replaceAll("\\?", "%3F");
            line.put(urlIndex, url);
        }
        String avinfo = mediaManager.getAvinfoBody(url);
        if (avinfo != null && !"".equals(avinfo)) {
            // 由于响应的 body 为多行需经过格式化处理为一行字符串
            try {
                return JsonUtils.toJson(avinfo);
            } catch (JsonParseException e) {
                throw new QiniuException(e, e.getMessage());
            }
        } else {
            throw new QiniuException(null, "empty_result");
        }
    }
}
