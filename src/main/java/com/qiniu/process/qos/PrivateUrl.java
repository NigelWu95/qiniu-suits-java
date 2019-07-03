package com.qiniu.process.qos;

import com.qiniu.common.QiniuException;
import com.qiniu.process.Base;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.Map;

public class PrivateUrl extends Base<Map<String, String>> {

    private Auth auth;
    private String domain;
    private String protocol;
    private String urlIndex;
    private long expires;

    public PrivateUrl(String accessKey, String secretKey, String domain, String protocol, String urlIndex, long expires,
                      String savePath, int saveIndex) throws IOException {
        super("privateurl", accessKey, secretKey, null, savePath, saveIndex);
        this.auth = Auth.create(accessKey, secretKey);
        set(domain, protocol, urlIndex, expires);
    }

    public PrivateUrl(String accessKey, String secretKey, String domain, String protocol, String urlIndex, long expires)
            throws IOException {
        super("privateurl", accessKey, secretKey, null);
        this.auth = Auth.create(accessKey, secretKey);
        set(domain, protocol, urlIndex, expires);
    }

    public PrivateUrl(String accessKey, String secretKey, String domain, String protocol, String urlIndex, long expires,
                      String savePath) throws IOException {
        this(accessKey, secretKey, domain, protocol, urlIndex, expires, savePath, 0);
    }

    private void set(String domain, String protocol, String urlIndex, long expires) throws IOException {
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
        this.expires = expires == 0L ? 3600 : expires;
    }

    public void updateDomain(String domain) {
        this.domain = domain;
    }

    public void updateProtocol(String protocol) {
        this.protocol = protocol;
    }

    public void updateUrlIndex(String urlIndex) {
        this.urlIndex = urlIndex;
    }

    public void updateExpires(long expires) {
        this.expires = expires;
    }

    public PrivateUrl clone() throws CloneNotSupportedException {
        PrivateUrl privateUrl = (PrivateUrl)super.clone();
        privateUrl.auth = Auth.create(authKey1, authKey2);
        return privateUrl;
    }

    @Override
    public String resultInfo(Map<String, String> line) {
        return line.get(urlIndex);
    }

    @Override
    public boolean validCheck(Map<String, String> line) {
        String url = line.get(urlIndex);
        return line.get("key") != null || (url != null && !url.isEmpty());
    }

    @Override
    public String singleResult(Map<String, String> line) throws QiniuException {
        String url = line.get(urlIndex);
        if (url == null || "".equals(url)) {
            url = protocol + "://" + domain + "/" + line.get("key").replaceAll("\\?", "%3f");
            line.put(urlIndex, url);
        }
        try {
            return auth.privateDownloadUrl(url, expires);
        } catch (Exception e) {
            throw new QiniuException(e, e.getMessage());
        }
    }
}
