package com.qiniu.service.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.persistence.FileMap;
import com.qiniu.storage.BucketManager;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.RequestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AsyncFetch implements ILineProcess<Map<String, String>>, Cloneable {

    final private Auth auth;
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
//    private M3U8Manager m3u8Manager;
    private boolean hasCustomArgs;
    private String host;
    private String callbackUrl;
    private String callbackBody;
    private String callbackBodyType;
    private String callbackHost;
    private int fileType;
    private boolean ignoreSameKey;
    final private String resultPath;
    private String resultTag;
    private int resultIndex;
    private FileMap fileMap;

    public AsyncFetch(Auth auth, Configuration configuration, String bucket, String domain, String protocol,
                      boolean srcPrivate, String keyPrefix, String urlIndex, String resultPath, int resultIndex)
            throws IOException {
        this.auth = auth;
        this.configuration = configuration;
        this.bucketManager = new BucketManager(auth, configuration);
        this.bucket = bucket;
        this.processName = "asyncfetch";
        setBatch(false);
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
//        this.m3u8Manager = new M3U8Manager();
        this.resultPath = resultPath;
        this.resultTag = "";
        this.resultIndex = resultIndex;
        this.fileMap = new FileMap(resultPath, processName, String.valueOf(resultIndex));
        this.fileMap.initDefaultWriters();
    }

    public AsyncFetch(Auth auth, Configuration configuration, String bucket, String domain, String protocol,
                      boolean srcPrivate, String keyPrefix, String urlIndex, String resultPath) throws IOException {
        this(auth, configuration, bucket, domain, protocol, srcPrivate, keyPrefix, urlIndex, resultPath, 0);
    }

    public void setFetchArgs(String md5Index, String host, String callbackUrl, String callbackBody, String callbackBodyType,
                             String callbackHost, int fileType, boolean ignoreSameKey) {
        this.md5Index = md5Index == null ? "" : md5Index;
        this.host = host;
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
        this.retryCount = retryCount;
    }

    public void setResultTag(String resultTag) {
        this.resultTag = resultTag == null ? "" : resultTag;
    }

    public AsyncFetch clone() throws CloneNotSupportedException {
        AsyncFetch asyncFetch = (AsyncFetch)super.clone();
        asyncFetch.bucketManager = new BucketManager(auth, configuration);
        asyncFetch.fileMap = new FileMap(resultPath, processName, resultTag + String.valueOf(++resultIndex));
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

    public String singleWithRetry(String url, String key, String md5, String etag, int retryCount) throws QiniuException {
        Response response = null;
        try {
            response = fetch(url, key, md5, etag);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    response = fetch(url, key, md5, etag);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }
        assert response != null;
        return response.reqId + "\t{\"code\":" + response.statusCode + ",\"message\":\"" +
                HttpResponseUtils.getResult(response) + "\"}";
    }

    public void processLine(List<Map<String, String>> lineList, int retryCount) throws IOException {
        String url;
        String key;
        String fetchResult;
        for (Map<String, String> line : lineList) {
            if (urlIndex != null) {
                url = line.get(urlIndex);
                key = url.split("(https?://[^\\s/]+\\.[^\\s/.]{1,3}/)|(\\?.+)")[1];
            } else  {
                url = protocol + "://" + domain + "/" + line.get("key");
                key = line.get("key");
            }
            try {
                fetchResult = singleWithRetry(url, keyPrefix + key, line.get(md5Index), line.get("hash"), retryCount);
                if (fetchResult != null && !"".equals(fetchResult))
                    fileMap.writeSuccess(key + "\t" + url + "\t" + fetchResult);
                else
                    fileMap.writeError( key + "\t" + url + "\t" + line.get(md5Index) +  "\t" + line.get("hash") +
                            "\tempty fetch result");
            } catch (QiniuException e) {
                String finalKey = key + "\t" + url;
                HttpResponseUtils.processException(e, fileMap, new ArrayList<String>(){{
                    add(finalKey + "\t" + line.get(md5Index) +  "\t" + line.get("hash"));
                }});
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
