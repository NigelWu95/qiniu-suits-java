package com.qiniu.service.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.persistence.FileMap;
import com.qiniu.storage.BucketManager;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AsyncFetch implements ILineProcess<Map<String, String>>, Cloneable {

    final private String accessKey;
    final private String secretKey;
    private Auth auth;
    final private Configuration configuration;
    private BucketManager bucketManager;
    final private String bucket;
    final private String processName;
    private int retryCount;
    private String domain;
    private String protocol;
    final private String urlIndex;
    private String md5Index;
    final private boolean srcPrivate;
    final private String keyPrefix;
    final private String rmPrefix;
//    private M3U8Manager m3u8Manager;
    private boolean hasCustomArgs;
    private String host;
    private String callbackUrl;
    private String callbackBody;
    private String callbackBodyType;
    private String callbackHost;
    private int fileType;
    private boolean ignoreSameKey;
    final private String savePath;
    private String saveTag;
    private int saveIndex;
    private FileMap fileMap;

    public AsyncFetch(String accessKey, String secretKey, Configuration configuration, String bucket, String domain,
                      String protocol, boolean srcPrivate, String keyPrefix, String rmPrefix, String urlIndex,
                      String savePath, int saveIndex) throws IOException {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.auth = Auth.create(accessKey, secretKey);
        this.configuration = configuration;
        this.bucketManager = new BucketManager(auth, configuration);
        this.bucket = bucket;
        this.processName = "asyncfetch";
        if (urlIndex == null || "".equals(urlIndex)) {
            this.urlIndex = null;
            if (domain == null || "".equals(domain)) throw new IOException("please set one of domain and urlIndex.");
            else {
                RequestUtils.checkHost(domain);
                this.domain = domain;
                this.protocol = protocol == null || !protocol.matches("(http|https)") ? "http" : protocol;
            }
        } else this.urlIndex = urlIndex;
        this.srcPrivate = srcPrivate;
        this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
        this.rmPrefix = rmPrefix;
//        this.m3u8Manager = new M3U8Manager();
        this.savePath = savePath;
        this.saveTag = "";
        this.saveIndex = saveIndex;
        this.fileMap = new FileMap(savePath, processName, String.valueOf(saveIndex));
        this.fileMap.initDefaultWriters();
    }

    public AsyncFetch(String accessKey, String secretKey, Configuration configuration, String bucket, String domain,
                      String protocol, boolean srcPrivate, String keyPrefix, String rmPrefix, String urlIndex,
                      String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, domain, protocol, srcPrivate, keyPrefix, rmPrefix, urlIndex,
                savePath, 0);
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

    public String getProcessName() {
        return this.processName;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount < 1 ? 1 : retryCount;
    }

    public void setSaveTag(String saveTag) {
        this.saveTag = saveTag == null ? "" : saveTag;
    }

    public AsyncFetch clone() throws CloneNotSupportedException {
        AsyncFetch asyncFetch = (AsyncFetch)super.clone();
        asyncFetch.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        asyncFetch.fileMap = new FileMap(savePath, processName, saveTag + String.valueOf(++saveIndex));
        try {
            asyncFetch.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return asyncFetch;
    }

    private Response fetch(String url, String key, String md5, String etag) throws QiniuException {
        if (srcPrivate) url = auth.privateDownloadUrl(url);
        return hasCustomArgs ?
                bucketManager.asynFetch(url, bucket, key, md5, etag, callbackUrl, callbackBody, callbackBodyType,
                        callbackHost, String.valueOf(fileType)) :
                bucketManager.asynFetch(url, bucket, key);
    }

    public void processLine(List<Map<String, String>> lineList, int retryCount) throws IOException {
        String url;
        String key;
        Response response;
        int retry;
        for (Map<String, String> line : lineList) {
            try {
                if (urlIndex != null) {
                    url = line.get(urlIndex);
                    key = URLUtils.getKey(url);
                } else {
                    key = line.get("key").replaceAll("\\?", "%3F");
                    url = protocol + "://" + domain + "/" + key;
                }
                key = FileNameUtils.rmPrefix(rmPrefix, key);
            } catch (IOException e) {
                fileMap.writeError(String.valueOf(line) + "\t" + e.getMessage(), false);
                continue;
            }
            String finalInfo = url + "\t" + key;
            retry = retryCount;
            while (retry > 0) {
                try {
                    response = fetch(url, keyPrefix + key, line.get(md5Index), line.get("hash"));
                    fileMap.writeSuccess(finalInfo + "\t" + HttpResponseUtils.responseJson(response) + "\t" +
                            response.reqId, false);
                    retry = 0;
                } catch (QiniuException e) {
                    retry = HttpResponseUtils.processException(e, retry, fileMap, new ArrayList<String>(){{
                        add(finalInfo);
                    }});
                }
            }
        }
    }

    public void processLine(List<Map<String, String>> lineList) throws IOException {
        processLine(lineList, retryCount);
    }

    public void closeResource() {
        fileMap.closeWriters();
    }
}
