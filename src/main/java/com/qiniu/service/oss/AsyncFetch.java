package com.qiniu.service.oss;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager.*;
import com.qiniu.service.auvideo.M3U8Manager;
import com.qiniu.service.auvideo.VideoTS;
import com.qiniu.service.interfaces.IOssFileProcess;
import com.qiniu.storage.Configuration;
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
    private Auth auth;
    private boolean keepKey;
    private String keyPrefix;
    private M3U8Manager m3u8Manager;

    public AsyncFetch(Auth auth, Configuration configuration, String bucket, boolean keepKey, String keyPrefix,
                      String resultFileDir, String processName, int resultFileIndex)
            throws IOException {
        super(auth, configuration, bucket, resultFileDir, processName, resultFileIndex);
        this.keepKey = keepKey;
        this.keyPrefix = keyPrefix;
        this.m3u8Manager = new M3U8Manager();
    }

    public AsyncFetch(Auth auth, Configuration configuration, String bucket, boolean keepKey, String keyPrefix,
                      String resultFileDir, String processName) throws IOException {
        this(auth, configuration, bucket, keepKey, keyPrefix, resultFileDir, processName, 0);
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

    protected Response getResponse(String key) throws QiniuException {
        String url = (https ? "https://" : "http://") + domain + "/" + key;
        if (auth != null) url = auth.privateDownloadUrl(url);
        return hasCustomArgs ?
                bucketManager.asynFetch(url, bucket, key, "", "", callbackUrl, callbackBody, callbackBodyType,
                        callbackHost, fileType) :
                bucketManager.asynFetch(url, bucket, key);
    }

    synchronized protected BatchOperations getOperations(List<String> keys){
        return new BatchOperations();
    }

    protected String getInfo() {
        return keyPrefix;
    }

    private void fetchTSByM3U8(String m3u8Url) {
        List<VideoTS> videoTSList = new ArrayList<>();
        try {
            videoTSList = m3u8Manager.getVideoTSListByUrl(m3u8Url);
        } catch (IOException ioException) {
            fileReaderAndWriterMap.writeErrorOrNull("list ts failed: " + m3u8Url);
        }

        for (VideoTS videoTS : videoTSList) {
//            processUrl(videoTS.getUrl(), videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1]);
        }
    }
}
