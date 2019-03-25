package com.qiniu.process.qoss;

import com.qiniu.process.Base;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.Map;

public class PrivateUrl extends Base {

    private String domain;
    private String protocol;
    private String urlIndex;
    private long expires;
    private Auth auth;

    public PrivateUrl(String accessKey, String secretKey, String domain, String protocol, String urlIndex, long expires,
                      String rmPrefix, String savePath, int saveIndex) throws IOException {
        super("privateurl", accessKey, secretKey, null, null, rmPrefix, savePath, saveIndex);
        if (urlIndex == null || "".equals(urlIndex)) {
            this.urlIndex = null;
            if (domain == null || "".equals(domain)) {
                throw new IOException("please set one of domain and urlIndex.");
            } else {
                RequestUtils.checkHost(domain);
                this.domain = domain;
                this.protocol = protocol == null || !protocol.matches("(http|https)") ? "http" : protocol;
            }
        } else {
            this.urlIndex = urlIndex;
        }
        this.expires = expires == 0L ? 3600 : expires;
        this.auth = Auth.create(accessKey, secretKey);
    }

    public PrivateUrl(String accessKey, String secretKey, String domain, String protocol, String urlIndex, long expires,
                      String rmPrefix, String savePath) throws IOException {
        this(accessKey, secretKey, domain, protocol, urlIndex, expires, rmPrefix, savePath, 0);
    }

    public PrivateUrl clone() throws CloneNotSupportedException {
        PrivateUrl privateUrl = (PrivateUrl)super.clone();
        privateUrl.auth = Auth.create(accessKey, secretKey);
        return privateUrl;
    }

    @Override
    protected Map<String, String> formatLine(Map<String, String> line) throws IOException {
        if (urlIndex == null) {
            line.put("key", FileNameUtils.rmPrefix(rmPrefix, line.get("key")));
            urlIndex = "url";
            line.put(urlIndex, protocol + "://" + domain + "/" + line.get("key").replaceAll("\\?", "%3F"));
        }
        return line;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get(urlIndex);
    }

    protected String singleResult(Map<String, String> line) {
        return auth.privateDownloadUrl(line.get(urlIndex), expires);
    }
}
