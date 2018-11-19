package com.qiniu.service.oss;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager.*;
import com.qiniu.service.auvideo.M3U8Manager;
import com.qiniu.service.auvideo.VideoTS;
import com.qiniu.service.interfaces.IOssFileProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AsyncFetch extends OperationBase implements IOssFileProcess, Cloneable {

    private String host;
    private String callbackUrl;
    private String callbackBody;
    private String callbackBodyType;
    private String callbackHost;
    private int fileType;
    private boolean ignoreSameKey;
    private boolean hasCustomArgs;
    private String domain;
    private boolean https;
    private Auth srcAuth;
    private boolean keepKey;
    private String keyPrefix;
    private boolean hashCheck;
    private M3U8Manager m3u8Manager;

    private void initOwnParams(String domain) {
        this.processName = "asyncfetch";
        this.domain = domain;
    }

    public AsyncFetch(Auth auth, Configuration configuration, String bucket, String domain, String resultFileDir,
                      int resultFileIndex) throws IOException {
        super(auth, configuration, bucket, resultFileDir);
        initOwnParams(domain);
        this.m3u8Manager = new M3U8Manager();
        this.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public AsyncFetch(Auth auth, Configuration configuration, String bucket, String domain, String resultFileDir) {
        super(auth, configuration, bucket, resultFileDir);
        initOwnParams(domain);
    }

    public void setFetchOptions(boolean keepKey, String keyPrefix) {
        this.keepKey = keepKey;
        this.keyPrefix = keyPrefix;
    }

    public void setUrlArgs(boolean https, Auth srcAuth) {
        this.https = https;
        this.srcAuth = srcAuth;
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
        asyncFetch.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        asyncFetch.m3u8Manager = new M3U8Manager();
        try {
            asyncFetch.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            e.printStackTrace();
            throw new CloneNotSupportedException();
        }
        return asyncFetch;
    }

    private Response fetch(String url, String key, String md5, String etag) throws QiniuException {
        return hasCustomArgs ?
                bucketManager.asynFetch(url, bucket, key, md5, etag, callbackUrl, callbackBody, callbackBodyType,
                        callbackHost, fileType) :
                bucketManager.asynFetch(url, bucket, key);
    }

    public Response intelligentlyFetch(String url, String key, String mimeType, String md5, String etag)
            throws QiniuException {
        if ("application/x-mpegurl".equals(mimeType) || key.endsWith(".m3u8")) {
            List<VideoTS> videoTSList = new ArrayList<>();
            try {
                videoTSList = m3u8Manager.getVideoTSListByUrl(url);
            } catch (IOException ioException) {
                fileReaderAndWriterMap.writeErrorOrNull("list ts failed: " + url);
            }

            for (VideoTS videoTS : videoTSList) {
                fetch(videoTS.getUrl(), keepKey ? keyPrefix +
                                videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?.+)")[1] : null,
                        "", "");
            }
        }
        return fetch(url, keepKey ? keyPrefix + key : null, md5, etag);
    }

    protected Response getResponse(FileInfo fileInfo) throws QiniuException {
        String url = (https ? "https://" : "http://") + domain + "/" + fileInfo.key;
        if (srcAuth != null) url = srcAuth.privateDownloadUrl(url);
        return intelligentlyFetch(url, fileInfo.key, fileInfo.mimeType, null, hashCheck ? fileInfo.hash : null);
    }

    synchronized protected BatchOperations getOperations(List<FileInfo> fileInfoList){
        return new BatchOperations();
    }

    protected String getInfo() {
        return keyPrefix;
    }
}
