package com.qiniu.process.qos;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.Map;

public class AsyncFetch extends Base<Map<String, String>> {

    private String domain;
    private String protocol;
    private String urlIndex;
    private String addPrefix;
    private String rmPrefix;
    private boolean hasCustomArgs;
    private String host;
    private String md5Index;
    private String callbackUrl;
    private String callbackBody;
    private String callbackBodyType;
    private String callbackHost;
    private int fileType;
    private boolean ignoreSameKey;
    private Configuration configuration;
    private BucketManager bucketManager;

    public AsyncFetch(String accessKey, String secretKey, Configuration configuration, String bucket, String domain,
                      String protocol, String urlIndex, String addPrefix, String rmPrefix) throws IOException {
        super("asyncfetch", accessKey, secretKey, bucket);
        set(configuration, domain, protocol, urlIndex, addPrefix, rmPrefix);
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
    }

    public AsyncFetch(String accessKey, String secretKey, Configuration configuration, String bucket, String domain,
                      String protocol, String urlIndex, String addPrefix, String rmPrefix, String savePath, int saveIndex)
            throws IOException {
        super("asyncfetch", accessKey, secretKey, bucket, savePath, saveIndex);
        set(configuration, domain, protocol, urlIndex, addPrefix, rmPrefix);
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
    }

    public AsyncFetch(String accessKey, String secretKey, Configuration configuration, String bucket, String domain,
                      String protocol, String urlIndex, String addPrefix, String rmPrefix, String savePath)
            throws IOException {
        this(accessKey, secretKey, configuration, bucket, domain, protocol, urlIndex, addPrefix, rmPrefix, savePath, 0);
    }

    private void set(Configuration configuration, String domain, String protocol, String urlIndex, String addPrefix,
                     String rmPrefix) throws IOException {
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
        this.addPrefix = addPrefix == null ? "" : addPrefix;
        this.rmPrefix = rmPrefix == null ? "" : rmPrefix;
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

    public void updateAddPrefix(String addPrefix) {
        this.addPrefix = addPrefix;
    }

    public void updateRmPrefix(String rmPrefix) {
        this.rmPrefix = rmPrefix;
    }

    public void setFetchArgs(String host, String md5Index, String callbackUrl, String callbackBody,
                             String callbackBodyType, String callbackHost, int fileType, boolean ignoreSameKey) {
        this.host = host;
        this.md5Index = md5Index;
        this.callbackUrl = callbackUrl;
        this.callbackBody = callbackBody;
        this.callbackBodyType = callbackBodyType;
        this.callbackHost = callbackHost;
        this.fileType = fileType;
        this.ignoreSameKey = ignoreSameKey;
        this.hasCustomArgs = true;
    }

    public AsyncFetch clone() throws CloneNotSupportedException {
        AsyncFetch asyncFetch = (AsyncFetch)super.clone();
        asyncFetch.bucketManager = new BucketManager(Auth.create(authKey1, authKey2), configuration.clone());
        return asyncFetch;
    }

    private Response fetch(String url, String key, String md5, String etag) throws QiniuException {
        return hasCustomArgs ?
                bucketManager.asynFetch(url, bucket, key, md5, etag, callbackUrl, callbackBody, callbackBodyType,
                        callbackHost, String.valueOf(fileType)) :
                bucketManager.asynFetch(url, bucket, key);
    }

    @Override
    public String resultInfo(Map<String, String> line) {
        return line.get("key") + "\t" + line.get(urlIndex);
    }

    @Override
    public boolean validCheck(Map<String, String> line) {
        String url = line.get(urlIndex);
        return line.get("key") != null || (url != null && !url.isEmpty());
    }

    @Override
    protected String singleResult(Map<String, String> line) throws QiniuException {
        String url = line.get(urlIndex);
        String key = line.get("key"); // 原始的认为正确的 key，用来拼接 URL 时需要保持不变
        try {
            if (url == null || "".equals(url)) {
                url = protocol + "://" + domain + "/" + key.replaceAll("\\?", "%3f");
                line.put(urlIndex, url);
                key = addPrefix + FileUtils.rmPrefix(rmPrefix, key); // 目标文件名
            } else {
                if (key != null) key = addPrefix + FileUtils.rmPrefix(rmPrefix, key);
                else key = addPrefix + FileUtils.rmPrefix(rmPrefix, URLUtils.getKey(url));
            }
            line.put("key", key);
        } catch (Exception e) {
            throw new QiniuException(e, e.getMessage());
        }
        Response response = fetch(url, key, line.get(md5Index), line.get("hash"));
        return key + "\t" + url + "\t" + response.statusCode + "\t" + HttpRespUtils.getResult(response);
    }

    @Override
    public void closeResource() {
        super.closeResource();
        domain = null;
        protocol = null;
        urlIndex = null;
        addPrefix = null;
        rmPrefix = null;
        host = null;
        md5Index = null;
        callbackUrl = null;
        callbackBody = null;
        callbackBodyType = null;
        callbackHost = null;
        configuration = null;
        bucketManager = null;
    }
}
