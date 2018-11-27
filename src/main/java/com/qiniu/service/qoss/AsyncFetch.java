package com.qiniu.service.qoss;

import com.qiniu.common.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager.*;
import com.qiniu.service.media.M3U8Manager;
import com.qiniu.service.media.VideoTS;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.RequestUtils;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class AsyncFetch extends OperationBase implements ILineProcess<FileInfo>, Cloneable {

    private String domain;
    private boolean https;
    private Auth srcAuth;
    private boolean keepKey;
    private String keyPrefix;
    private boolean hashCheck;
    private M3U8Manager m3u8Manager;
    private boolean hasCustomArgs;
    private String host;
    private String callbackUrl;
    private String callbackBody;
    private String callbackBodyType;
    private String callbackHost;
    private int fileType;
    private boolean ignoreSameKey;

    private void initBaseParams(String domain) throws UnknownHostException {
        this.processName = "asyncfetch";
        this.domain = domain;
        RequestUtils.checkHost(domain);
    }

    public AsyncFetch(Auth auth, Configuration configuration, String bucket, String domain, String resultFileDir,
                      int resultFileIndex) throws IOException {
        super(auth, configuration, bucket, resultFileDir);
        initBaseParams(domain);
        this.m3u8Manager = new M3U8Manager();
        this.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public AsyncFetch(Auth auth, Configuration configuration, String bucket, String domain, String resultFileDir)
            throws IOException {
        super(auth, configuration, bucket, resultFileDir);
        initBaseParams(domain);
    }

    public void setOptions(boolean https, Auth srcAuth, boolean keepKey, String keyPrefix, boolean hashCheck) {
        this.https = https;
        this.srcAuth = srcAuth;
        this.keepKey = keepKey;
        this.keyPrefix = keyPrefix;
        this.hashCheck = hashCheck;
    }

    public void setFetchArgs(String host, String callbackUrl, String callbackBody, String callbackBodyType,
                             String callbackHost, int fileType, boolean ignoreSameKey) {
        this.host = host;
        this.callbackUrl = callbackUrl;
        this.callbackBody = callbackBody;
        this.callbackBodyType = callbackBodyType;
        this.callbackHost = callbackHost;
        this.fileType = fileType;
        this.ignoreSameKey = ignoreSameKey;
        this.hasCustomArgs = true;
    }

    public AsyncFetch getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        AsyncFetch asyncFetch = (AsyncFetch)super.clone();
        asyncFetch.fileMap = new FileMap();
        asyncFetch.m3u8Manager = new M3U8Manager();
        try {
            asyncFetch.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return asyncFetch;
    }

    public String getInfo() {
        return domain + "\t" + https + "\t" + !(srcAuth == null) + "\t" + keepKey + "\t" + keyPrefix + "\t" +
                hashCheck + (!hasCustomArgs ? "" : "\t" +
                host + "\t" + callbackUrl + "\t" + callbackBody + "\t" + callbackBodyType + "\t" + callbackHost +
                fileType + "\t" + ignoreSameKey);
    }

    private Response fetch(String url, String key, String md5, String etag) throws QiniuException {
        if (srcAuth != null) url = srcAuth.privateDownloadUrl(url);
        return hasCustomArgs ?
                bucketManager.asynFetch(url, bucket, key, md5, etag, callbackUrl, callbackBody, callbackBodyType,
                        callbackHost, fileType) :
                bucketManager.asynFetch(url, bucket, key);
    }

    protected Response getResponse(FileInfo fileInfo) throws QiniuException {
        String url = (https ? "https://" : "http://") + domain + "/" + fileInfo.key;
        if (srcAuth != null) url = srcAuth.privateDownloadUrl(url);
        Response response = fetch(url, keepKey ? keyPrefix + fileInfo.key : null,
                null, hashCheck ? fileInfo.hash : null);
        if ("application/x-mpegurl".equals(fileInfo.mimeType) || fileInfo.key.endsWith(".m3u8")) {
            List<VideoTS> videoTSList = new ArrayList<>();
            try {
                videoTSList = m3u8Manager.getVideoTSListByUrl(url);
            } catch (IOException ioException) {
                fileMap.writeErrorOrNull("list ts failed: " + url);
            }

            for (VideoTS videoTS : videoTSList) {
                String key = videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?.+)")[1];
                fetch(videoTS.getUrl(), keepKey ? keyPrefix + key : null, "", "");
            }
        }

        return response;
    }

    synchronized protected BatchOperations getOperations(List<FileInfo> fileInfoList){
        return null;
    }
}
