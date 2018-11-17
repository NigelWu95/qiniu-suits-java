package com.qiniu.service.oss;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.model.FetchBody;
import com.qiniu.model.FetchFile;
import com.qiniu.sdk.BucketManager;
import com.qiniu.sdk.BucketManager.*;
import com.qiniu.service.auvideo.M3U8Manager;
import com.qiniu.service.interfaces.IOssFileProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class AsyncFetch extends OperationBase implements IOssFileProcess, Cloneable {

    private FetchBody fetchBody;
    private boolean keepKey;
    private String keyPrefix;
    private M3U8Manager m3u8Manager = new M3U8Manager();

    public AsyncFetch(Auth auth, Configuration configuration, FetchBody fetchBody, String resultFileDir,
                      String processName, int resultFileIndex)
            throws IOException {
        super(auth, configuration, resultFileDir, processName, resultFileIndex);
        this.fetchBody = fetchBody;
    }

    public AsyncFetch(Auth auth, Configuration configuration, FetchBody fetchBody, String resultFileDir,
                      String processName) throws IOException {
        this(auth, configuration, fetchBody, resultFileDir, processName, 0);
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

    public String getProcessName() {
        return this.processName;
    }

    public Response singleWithRetry(String key, int retryCount)
            throws QiniuException {

        Response response = null;
        FetchFile fetchFile = fetchBody.fetchFiles.get(0);
        try {
            response = fetchBody.hasCustomArgs() ?
                    bucketManager.asynFetch(fetchFile.url, fetchBody.bucket, fetchFile.key, fetchFile.md5,
                            fetchFile.etag, fetchBody.callbackUrl, fetchBody.callbackBody, fetchBody.callbackBodyType,
                            fetchBody.callbackHost, fetchBody.fileType) :
                    bucketManager.asynFetch(fetchFile.url, fetchBody.bucket, fetchFile.key);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    response = bucketManager.asynFetch(fetchFile.url, fetchBody.bucket, fetchFile.key);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }

    protected BucketManager.BatchOperations getOperations(List<String> keys){
        return new BatchOperations();
    }

    protected String getInfo() {
        return keyPrefix;
    }

//    public AsyncFetch(Auth auth, String targetBucket, String resultFileDir, String processName) throws IOException {
//        this.processName = processName;
//        this.fileReaderAndWriterMap.initWriter(resultFileDir, processName, null);
//    }
//
//    public AsyncFetch(Auth auth, String targetBucket, String resultFileDir, String processName, M3U8Manager m3u8Manager)
//            throws IOException {
//        this(auth, targetBucket, resultFileDir, processName);
//        this.m3u8Manager = m3u8Manager;
//    }

    private void fetchResult(String url, String key) throws QiniuException {
        try {
//            String fetchResult = asyncFetch.run(url, key, 0);
            String fetchResult = run(null, 0);
            fileReaderAndWriterMap.writeSuccess(fetchResult);
        } catch (QiniuException e) {
            fileReaderAndWriterMap.writeErrorOrNull(url + "," + key + "\t" + e.error());
            if (!e.response.needRetry()) throw e;
            else e.response.close();
        }
    }

//    public void processItem(String source, String item) {
//        processItem(source, item, item);
//    }

//    public void processItem(String source, String item, String key) {
//        String url = source.endsWith("/") ? source + item : source + "/" + item;
//        fetchResult(url, key);
//    }
//
//    public void processItem(Auth auth, String source, String item) {
//        processItem(auth, source, item, item);
//    }
//
//    public void processItem(Auth auth, String source, String item, String key) {
//        String url = auth.privateDownloadUrl(source + item);
//        fetchResult(url, key);
//    }
//
//    public void processUrl(String url, String key) {
//        fetchResult(url, key);
//    }
//
//    public void processUrl(String url, String key, String format) {
//        processUrl(url, key);
//
//        if (Arrays.asList("hls", "HLS", "m3u8", "M3U8").contains(format)) {
//            fetchTSByM3U8(url);
//        }
//    }
//
//    public void processUrl(Auth auth, String url, String key) {
//        url = auth.privateDownloadUrl(url);
//        fetchResult(url, key);
//    }

//    public void processUrl(Auth auth, String url, String key, String format) {
//        url = auth.privateDownloadUrl(url);
//        processUrl(url, key);
//
//        if (Arrays.asList("hls", "HLS", "m3u8", "M3U8").contains(format)) {
//            fetchTSByM3U8(url);
//        }
//    }
//
//    private void fetchTSByM3U8(String rootUrl, String m3u8FilePath) {
//        List<VideoTS> videoTSList = new ArrayList<>();
//
//        try {
//            videoTSList = m3u8Manager.getVideoTSListByFile(rootUrl, m3u8FilePath);
//        } catch (IOException ioException) {
//            fileReaderAndWriterMap.writeOther("list ts failed: " + m3u8FilePath);
//        }
//
//        for (VideoTS videoTS : videoTSList) {
//            processUrl(videoTS.getUrl(), videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1]);
//        }
//    }
//
//    private void fetchTSByM3U8(String m3u8Url) {
//        List<VideoTS> videoTSList = new ArrayList<>();
//
//        try {
//            videoTSList = m3u8Manager.getVideoTSListByUrl(m3u8Url);
//        } catch (IOException ioException) {
//            fileReaderAndWriterMap.writeErrorOrNull("list ts failed: " + m3u8Url);
//        }
//
//        for (VideoTS videoTS : videoTSList) {
//            processUrl(videoTS.getUrl(), videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1]);
//        }
//    }
}
