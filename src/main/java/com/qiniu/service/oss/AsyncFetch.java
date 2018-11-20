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

    private void initBaseParams(String domain) {
        this.processName = "asyncfetch";
        this.domain = domain;
    }

    public AsyncFetch(Auth auth, Configuration configuration, String bucket, String domain, String resultFileDir,
                      int resultFileIndex) throws IOException {
        super(auth, configuration, bucket, resultFileDir);
        initBaseParams(domain);
        this.m3u8Manager = new M3U8Manager();
        this.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public AsyncFetch(Auth auth, Configuration configuration, String bucket, String domain, String resultFileDir) {
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
                fileReaderAndWriterMap.writeErrorOrNull("list ts failed: " + url);
            }

            for (VideoTS videoTS : videoTSList) {
                String key = videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?.+)")[1];
                fetch(videoTS.getUrl(), keepKey ? keyPrefix + key : null, "", "");
            }
        }
        return response;
    }

    synchronized protected BatchOperations getOperations(List<FileInfo> fileInfoList){
        return new BatchOperations();
    }

    protected String getInfo() {
        return keyPrefix;
    }
}
