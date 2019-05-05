package com.qiniu.process.qoss;

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
    private BucketManager bucketManager;

    public AsyncFetch(String accessKey, String secretKey, Configuration configuration, String bucket, String domain,
                      String protocol, String urlIndex, String addPrefix, String savePath,
                      int saveIndex) throws IOException {
        super("asyncfetch", accessKey, secretKey, configuration, bucket, savePath, saveIndex);
        set(domain, protocol, urlIndex, addPrefix);
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
    }

    public void updateFetch(String bucket, String domain, String protocol, String urlIndex, String keyPrefix,
                            String rmPrefix) throws IOException {
        this.bucket = bucket;
        set(domain, protocol, urlIndex, keyPrefix);
        this.rmPrefix = rmPrefix;
    }

    private void set(String domain, String protocol, String urlIndex, String addPrefix) throws IOException {
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
        this.addPrefix = addPrefix == null ? "" : addPrefix;
    }

    public AsyncFetch(String accessKey, String secretKey, Configuration configuration, String bucket, String domain,
                      String protocol, String urlIndex, String keyPrefix, String savePath)
            throws IOException {
        this(accessKey, secretKey, configuration, bucket, domain, protocol, urlIndex, keyPrefix, savePath, 0);
    }

    public void setFetchArgs(String host, String md5Index, String callbackUrl, String callbackBody,
                             String callbackBodyType, String callbackHost, int fileType, boolean ignoreSameKey) {
        this.host = host;
        this.md5Index = md5Index == null ? "" : md5Index;
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
        asyncFetch.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        return asyncFetch;
    }

    private Response fetch(String url, String key, String md5, String etag) throws QiniuException {
        return hasCustomArgs ?
                bucketManager.asynFetch(url, bucket, key, md5, etag, callbackUrl, callbackBody, callbackBodyType,
                        callbackHost, String.valueOf(fileType)) :
                bucketManager.asynFetch(url, bucket, key);
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get("key") + "\t" + line.get(urlIndex);
    }

    @Override
    protected boolean validCheck(Map<String, String> line) {
        return line.get("key") != null;
    }

    @Override
    protected String singleResult(Map<String, String> line) throws QiniuException {
        String url = line.get(urlIndex);
        try {
            String key;
            if (url == null || "".equals(url)) {
                key = line.get("key").replaceAll("\\?", "%3F");
                url = protocol + "://" + domain + "/" + key;
                line.put(urlIndex, url);
            } else {
                key = FileNameUtils.rmPrefix(rmPrefix, URLUtils.getKey(url));
            }
            line.put("key", addPrefix + key);
        } catch (Exception e) {
            throw new QiniuException(e, e.getMessage());
        }
        Response response = fetch(url, line.get("key"), line.get(md5Index), line.get("hash"));
        return HttpResponseUtils.responseJson(response);
    }
}
