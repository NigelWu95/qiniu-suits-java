package com.qiniu.process.qiniu;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.interfaces.IFileChecker;
import com.qiniu.process.Base;
import com.qiniu.process.other.HttpDownloader;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.UploadManager;
import com.qiniu.util.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class SyncUpload extends Base<Map<String, String>> {

    private String protocol;
    private String domain;
    private String urlIndex;
    private StringMap headers;
    private String addPrefix;
    private String rmPrefix;
    private Auth auth;
    private StringMap params;
    private long expires;
    private StringMap policy;
    private Configuration configuration;
    private UploadManager uploadManager;
    private HttpDownloader downloader;

    public SyncUpload(String accessKey, String secretKey, Configuration configuration, String protocol, String domain,
                      String urlIndex, String host, String addPrefix, String rmPrefix, String bucket, long expires,
                      StringMap policy, StringMap params) throws IOException {
        super("syncupload", accessKey, secretKey, bucket);
        CloudApiUtils.checkQiniu(accessKey, secretKey, configuration, bucket);
        auth = Auth.create(accessKey, secretKey);
        Configuration config = configuration.clone();
        uploadManager = new UploadManager(config);
        downloader = new HttpDownloader(config);
        set(configuration, protocol, domain, urlIndex, host, addPrefix, rmPrefix, expires, policy, params);
    }

    public SyncUpload(String accessKey, String secretKey, Configuration configuration, String protocol, String domain,
                      String urlIndex, String host, String addPrefix, String rmPrefix, String bucket, long expires,
                      StringMap policy, StringMap params, String savePath, int saveIndex) throws IOException {
        super("syncupload", accessKey, secretKey, bucket, savePath, saveIndex);
        CloudApiUtils.checkQiniu(accessKey, secretKey, configuration, bucket);
        auth = Auth.create(accessKey, secretKey);
        Configuration config = configuration.clone();
        uploadManager = new UploadManager(config);
        downloader = new HttpDownloader(config);
        set(configuration, protocol, domain, urlIndex, host, addPrefix, rmPrefix, expires, policy, params);
    }

    public SyncUpload(String accessKey, String secretKey, Configuration configuration, String protocol, String domain,
                      String urlIndex, String host, String addPrefix, String rmPrefix, String bucket, long expires,
                      StringMap policy, StringMap params, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, protocol, domain, urlIndex, host, addPrefix, rmPrefix, bucket, expires,
                policy, params, savePath, 0);
    }

    private void set(Configuration configuration, String protocol, String domain, String urlIndex, String host,
                     String addPrefix, String rmPrefix, long expires, StringMap policy, StringMap params) throws IOException {
        this.configuration = configuration;
        if (urlIndex == null || "".equals(urlIndex)) {
            this.urlIndex = "url";
            if (domain == null || "".equals(domain)) {
                throw new IOException("please set one of domain and url-index.");
            } else {
                this.protocol = protocol == null || !protocol.matches("(http|https)") ? "http" : protocol;
                RequestUtils.lookUpFirstIpFromHost(domain);
                this.domain = domain;
            }
        } else {
            this.urlIndex = urlIndex;
        }
        if (host != null && !"".equals(host)) {
            RequestUtils.lookUpFirstIpFromHost(host);
            headers = new StringMap().put("Host", host);
        }
        this.addPrefix = addPrefix == null ? "" : addPrefix;
        this.rmPrefix = rmPrefix;
        this.addPrefix = addPrefix == null ? "" : addPrefix;
        this.rmPrefix = rmPrefix;
        this.expires = expires;
        this.policy = policy;
        this.params = params;
    }

    @Override
    public SyncUpload clone() throws CloneNotSupportedException {
        SyncUpload syncUpload = (SyncUpload)super.clone();
        syncUpload.auth = Auth.create(accessId, secretKey);
        Configuration config = configuration.clone();
        syncUpload.uploadManager = new UploadManager(config);
        syncUpload.downloader = new HttpDownloader(config);
        return syncUpload;
    }

    @Override
    protected IFileChecker fileCheckerInstance() {
        return "stat".equals(checkType) ? CloudApiUtils.fileCheckerInstance(new BucketManager(auth, configuration), bucket)
                : key -> null;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return String.join("\t", line.get("key"), line.get(urlIndex));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws Exception {
        String url = line.get(urlIndex);
        String key = line.get("key");
        if (url == null || "".equals(url)) {
            if (key == null || "".equals(key)) throw new IOException("key is not exists or empty in " + line);
            url = String.join("", protocol, "://", domain, "/", key.replace("\\?", "%3f"));
            line.put(urlIndex, url);
            key = String.join("", addPrefix, FileUtils.rmPrefix(rmPrefix, key)); // 目标文件名
        } else {
            if (key != null) key = String.join("", addPrefix, FileUtils.rmPrefix(rmPrefix, key));
            else key = String.join("", addPrefix, FileUtils.rmPrefix(rmPrefix, URLUtils.getKey(url)));
        }
        line.put("key", key);
        if (iFileChecker.check(key) != null) throw new IOException("file exists");
        Response response = downloader.downloadResponse(url, headers);
        if (response.statusCode == 200 || response.statusCode == 206) {
            try {
                InputStream stream = response.bodyStream();
                return String.join("\t", url, HttpRespUtils.getResult(uploadManager.put(stream, key,
                        auth.uploadToken(bucket, key, expires, policy), params, response.contentType())));
            } finally {
                response.close();
            }
        } else {
            throw new QiniuException(response);
        }
    }

    @Override
    public void closeResource() {
        super.closeResource();
        protocol = null;
        domain = null;
        urlIndex = null;
        headers = null;
        addPrefix = null;
        rmPrefix = null;
        auth = null;
        policy = null;
        params = null;
        configuration = null;
        uploadManager = null;
        downloader = null;
    }
}
