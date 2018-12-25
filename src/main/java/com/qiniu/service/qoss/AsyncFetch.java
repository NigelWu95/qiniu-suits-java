package com.qiniu.service.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.service.media.M3U8Manager;
import com.qiniu.service.media.VideoTS;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.RequestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AsyncFetch extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    private String domain;
    private String protocol;
    private String urlIndex;
    private String md5Index;
    private Auth srcAuth;
    private boolean keepKey;
    private String keyPrefix;
    private M3U8Manager m3u8Manager;
    private boolean hasCustomArgs;
    private String host;
    private String callbackUrl;
    private String callbackBody;
    private String callbackBodyType;
    private String callbackHost;
    private int fileType;
    private boolean ignoreSameKey;

    public AsyncFetch(Auth auth, Configuration configuration, String bucket, String domain, String protocol, Auth srcAuth,
                      boolean keepKey, String keyPrefix, String urlIndex, String resultPath, int resultIndex)
            throws IOException {
        super(auth, configuration, bucket, "asyncfetch", resultPath, resultIndex);
        setBatch(false);
        if (domain == null || "".equals(domain)) this.domain = null;
        else {
            RequestUtils.checkHost(domain);
            this.domain = domain;
        }
        this.protocol = protocol;
        this.urlIndex = urlIndex;
        this.srcAuth = srcAuth;
        this.keepKey = keepKey;
        this.keyPrefix = keyPrefix;
        this.m3u8Manager = new M3U8Manager();
    }

    public AsyncFetch(Auth auth, Configuration configuration, String bucket, String domain, String protocol, Auth srcAuth,
                      boolean keepKey, String keyPrefix, String urlIndex, String resultFileDir)
            throws IOException {
        this(auth, configuration, bucket, domain, protocol, srcAuth, keepKey, keyPrefix, urlIndex, resultFileDir, 0);
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

    public AsyncFetch clone() throws CloneNotSupportedException {
        AsyncFetch asyncFetch = (AsyncFetch)super.clone();
        asyncFetch.m3u8Manager = new M3U8Manager();
        return asyncFetch;
    }

    private Response fetch(String url, String key, String md5, String etag) throws QiniuException {
        if (srcAuth != null) url = srcAuth.privateDownloadUrl(url);
        return hasCustomArgs ?
                bucketManager.asynFetch(url, bucket, key, md5, etag, callbackUrl, callbackBody, callbackBodyType,
                        callbackHost, String.valueOf(fileType)) :
                bucketManager.asynFetch(url, bucket, key);
    }

    protected String processLine(Map<String, String> line) throws QiniuException {
        String url;
        String key;
        if (domain != null) {
            url = protocol + "://" + domain + "/" + line.get("key");
            key = line.get("key");
        } else {
            url = line.get(urlIndex);
            key = url.split("(https?://[^\\s/]+\\.[^\\s/.]{1,3}/)|(\\?.+)")[1];
        }
        Response response = fetch(url, keepKey ? keyPrefix + key : null, line.get(md5Index), line.get("hash"));
        if ("application/x-mpegurl".equals(line.get("mimeType")) || key.endsWith(".m3u8")) {
            List<VideoTS> videoTSList = new ArrayList<>();
            try {
                videoTSList = m3u8Manager.getVideoTSListByUrl(url);
            } catch (IOException ioException) {
                fileMap.writeErrorOrNull("list ts failed: " + url);
            }
            for (VideoTS videoTS : videoTSList) {
                key = videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/.]{1,3}/)|(\\?.+)")[1];
                fetch(videoTS.getUrl(), keepKey ? keyPrefix + key : null, line.get(md5Index), line.get("hash"));
            }
        }
        return response.statusCode + "\t" + HttpResponseUtils.getResult(response);
    }

    synchronized protected BatchOperations getOperations(List<Map<String, String>> fileInfoList){
        return new BatchOperations();
    }
}
