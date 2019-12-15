package com.qiniu.process.qiniu;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.interfaces.IFileChecker;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.Map;

public class FetchFile extends Base<Map<String, String>> {

    private String protocol;
    private String domain;
    private String urlIndex;
    private String addPrefix;
    private String rmPrefix;
    private Configuration configuration;
    private BucketManager bucketManager;

    public FetchFile(String accessKey, String secretKey, Configuration configuration, String bucket, String protocol,
                     String domain, String urlIndex, String addPrefix, String rmPrefix) throws IOException {
        super("fetch", accessKey, secretKey, bucket);
        this.configuration = configuration;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        CloudApiUtils.checkQiniu(bucketManager, bucket);
        set(configuration, protocol, domain, urlIndex, addPrefix, rmPrefix);
    }

    public FetchFile(String accessKey, String secretKey, Configuration configuration, String bucket, String protocol,
                     String domain, String urlIndex, String addPrefix, String rmPrefix, String savePath, int saveIndex)
            throws IOException {
        super("fetch", accessKey, secretKey, bucket, savePath, saveIndex);
        this.configuration = configuration;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        CloudApiUtils.checkQiniu(bucketManager, bucket);
        set(configuration, protocol, domain, urlIndex, addPrefix, rmPrefix);
    }

    public FetchFile(String accessKey, String secretKey, Configuration configuration, String bucket, String protocol,
                     String domain, String urlIndex, String addPrefix, String rmPrefix, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, protocol, domain, urlIndex, addPrefix, rmPrefix, savePath, 0);
    }

    private void set(Configuration configuration, String protocol, String domain, String urlIndex, String addPrefix,
                     String rmPrefix) throws IOException {
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
        this.addPrefix = addPrefix == null ? "" : addPrefix;
        this.rmPrefix = rmPrefix;
    }

    @Override
    public FetchFile clone() throws CloneNotSupportedException {
        FetchFile fetchFile = (FetchFile) super.clone();
        fetchFile.bucketManager = new BucketManager(Auth.create(accessId, secretKey), configuration.clone());
        return fetchFile;
    }

    @Override
    protected IFileChecker fileCheckerInstance() {
        return "stat".equals(checkType) ? key -> {
            Response response;
            try {
                response = bucketManager.statResponse(bucket, key);
            } catch (QiniuException e) {
                if (e.response != null) e.response.close();
                return null;
            }
            if (response.statusCode == 200) {
                try {
                    return response.bodyString();
                } finally {
                    response.close();
                }
            }
            return null;
        } : key -> null;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return String.join("\t", line.get("key"), line.get(urlIndex));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws IOException {
        String url = line.get(urlIndex);
        String key = line.get("key"); // 原始的认为正确的 key，用来拼接 URL 时需要保持不变
        if (url == null || "".equals(url)) {
            if (key == null) throw new IOException("key is not exists or empty in " + line);
            url = String.join("", protocol, "://", domain, "/", key.replace("\\?", "%3f"));
            line.put(urlIndex, url);
            key = String.join("", addPrefix, FileUtils.rmPrefix(rmPrefix, key)); // 目标文件名
        } else {
            if (key != null) key = String.join("", addPrefix, FileUtils.rmPrefix(rmPrefix, key));
            else key = String.join("", addPrefix, FileUtils.rmPrefix(rmPrefix, URLUtils.getKey(url)));
        }
        line.put("key", key);
        Response response = bucketManager.fetchResponse(url, bucket, key);
        return String.join("\t", key, url, String.valueOf(response.statusCode), HttpRespUtils.getResult(response));
    }

    @Override
    public void closeResource() {
        super.closeResource();
        protocol = null;
        domain = null;
        urlIndex = null;
        addPrefix = null;
        rmPrefix = null;
        configuration = null;
        bucketManager = null;
    }
}
