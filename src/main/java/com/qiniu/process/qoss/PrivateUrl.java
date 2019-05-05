package com.qiniu.process.qoss;

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
        super("privateurl", accessKey, secretKey, null, null, savePath, saveIndex);
        this.auth = Auth.create(accessKey, secretKey);
        set(domain, protocol, urlIndex, expires);
    }

    public void updatePrivate(String domain, String protocol, String urlIndex, long expires, String rmPrefix)
            throws IOException {
        set(domain, protocol, urlIndex, expires);
    }

    private void set(String domain, String protocol, String urlIndex, long expires) throws IOException {
        if (urlIndex == null || "".equals(urlIndex)) {
            this.urlIndex = "url";
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
    }

    public PrivateUrl(String accessKey, String secretKey, String domain, String protocol, String urlIndex, long expires,
                      String savePath) throws IOException {
        this(accessKey, secretKey, domain, protocol, urlIndex, expires, savePath, 0);
    }

    public PrivateUrl clone() throws CloneNotSupportedException {
        PrivateUrl privateUrl = (PrivateUrl)super.clone();
        privateUrl.auth = Auth.create(accessKey, secretKey);
        return privateUrl;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get(urlIndex);
    }

    @Override
    protected boolean validCheck(Map<String, String> line) {
        return line.get("key") != null;
    }

    @Override
    protected void parseSingleResult(Map<String, String> line, String result) throws IOException {
        fileSaveMapper.writeSuccess(result, false);
    }

    @Override
    protected String singleResult(Map<String, String> line) throws QiniuException {
        String url = line.get(urlIndex);
        if (url == null || "".equals(url)) url = protocol + "://" + domain + "/" + line.get("key").replaceAll("\\?", "%3F");
        try {
            return auth.privateDownloadUrl(url, expires);
        } catch (Exception e) {
            throw new QiniuException(e, e.getMessage());
        }
    }
}
