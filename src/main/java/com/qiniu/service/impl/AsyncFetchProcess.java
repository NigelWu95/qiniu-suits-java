package com.qiniu.service.impl;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.model.FetchBody;
import com.qiniu.service.auvideo.M3U8Manager;
import com.qiniu.service.auvideo.VideoTS;
import com.qiniu.interfaces.IUrlItemProcess;
import com.qiniu.service.oss.AsyncFetch;
import com.qiniu.service.oss.BucketCopy;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.JsonConvertUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AsyncFetchProcess implements IUrlItemProcess, IOssFileProcess, Cloneable {

    private AsyncFetch asyncFetch;
    private String resultFileDir;
    private String processName;
    private FileReaderAndWriterMap fileReaderAndWriterMap = new FileReaderAndWriterMap();
    private FetchBody fetchBody;
    private QiniuException qiniuException = null;

    private M3U8Manager m3u8Manager;

    public AsyncFetchProcess(Auth auth, Configuration configuration, FetchBody fetchBody, String resultFileDir,
                             String processName, int resultFileIndex)
            throws IOException {
        this.asyncFetch = new AsyncFetch(auth, configuration);
        this.resultFileDir = resultFileDir;
        this.processName = processName;
        this.fetchBody = fetchBody;
        this.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public AsyncFetchProcess(Auth auth, Configuration configuration, FetchBody fetchBody, String resultFileDir,
                             String processName) throws IOException {
        this(auth, configuration, fetchBody, resultFileDir, processName, 0);
    }

    public AsyncFetchProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        AsyncFetchProcess asyncFetchProcess = (AsyncFetchProcess)super.clone();
        asyncFetchProcess.asyncFetch = asyncFetch.clone();
        asyncFetchProcess.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        try {
            asyncFetchProcess.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            e.printStackTrace();
            throw new CloneNotSupportedException();
        }
        return asyncFetchProcess;
    }

    public String getProcessName() {
        return this.processName;
    }

    public QiniuException qiniuException() {
        return qiniuException;
    }

    public void processFile(String fileKey, int retryCount) {

        try {
            String result = asyncFetch.run(fetchBody, false, "", retryCount);
            if (!StringUtils.isNullOrEmpty(result)) fileReaderAndWriterMap.writeSuccess(result);
        } catch (QiniuException e) {
            fileReaderAndWriterMap.writeErrorOrNull(JsonConvertUtils.toJson(fetchBody) + "\t" + e.error());
            if (!e.response.needRetry()) qiniuException = e;
            else e.response.close();
        }
    }

    public void processFile(List<String> keyList, int retryCount) {

//        if (keyList == null || keyList.size() == 0) return;
//        int times = keyList.size()/1000 + 1;
//        for (int i = 0; i < times; i++) {
//            List<String> processList = keyList.subList(1000 * i, i == times - 1 ? keyList.size() : 1000 * (i + 1));
//            if (processList.size() > 0) {
//                try {
//                    String result = bucketCopy.batchRun(srcBucket, tarBucket, processList, keyPrefix, false,
//                            retryCount);
//                    if (!StringUtils.isNullOrEmpty(result)) fileReaderAndWriterMap.writeSuccess(result);
//                } catch (QiniuException e) {
//                    fileReaderAndWriterMap.writeErrorOrNull(srcBucket + "\t" + tarBucket + "\t" + keyPrefix + "\t"
//                            + processList + "\t" + false + "\t" + e.error());
//                    if (!e.response.needRetry()) qiniuException = e;
//                    else e.response.close();
//                }
//            }
//        }
    }

    public AsyncFetchProcess(Auth auth, String targetBucket, String resultFileDir, String processName) throws IOException {
        this.asyncFetch = new AsyncFetch(auth, null);
        this.processName = processName;
        this.fileReaderAndWriterMap.initWriter(resultFileDir, processName, null);
    }

    public AsyncFetchProcess(Auth auth, String targetBucket, String resultFileDir, String processName, M3U8Manager m3u8Manager)
            throws IOException {
        this(auth, targetBucket, resultFileDir, processName);
        this.m3u8Manager = m3u8Manager;
    }

    private void fetchResult(String url, String key) {
        try {
//            String fetchResult = asyncFetch.run(url, key, 0);
            String fetchResult = asyncFetch.run(null, true, "", 0);
            fileReaderAndWriterMap.writeSuccess(fetchResult);
        } catch (QiniuException e) {
            fileReaderAndWriterMap.writeErrorOrNull(url + "," + key + "\t" + e.error());
            if (!e.response.needRetry()) qiniuException = e;
            else e.response.close();
        }
    }

    public void processItem(String source, String item) {
        processItem(source, item, item);
    }

    public void processItem(String source, String item, String key) {
        String url = source.endsWith("/") ? source + item : source + "/" + item;
        fetchResult(url, key);
    }

    public void processItem(Auth auth, String source, String item) {
        processItem(auth, source, item, item);
    }

    public void processItem(Auth auth, String source, String item, String key) {
        String url = auth.privateDownloadUrl(source + item);
        fetchResult(url, key);
    }

    public void processUrl(String url, String key) {
        fetchResult(url, key);
    }

    public void processUrl(String url, String key, String format) {
        processUrl(url, key);

        if (Arrays.asList("hls", "HLS", "m3u8", "M3U8").contains(format)) {
            fetchTSByM3U8(url);
        }
    }

    public void processUrl(Auth auth, String url, String key) {
        url = auth.privateDownloadUrl(url);
        fetchResult(url, key);
    }

    public void processUrl(Auth auth, String url, String key, String format) {
        url = auth.privateDownloadUrl(url);
        processUrl(url, key);

        if (Arrays.asList("hls", "HLS", "m3u8", "M3U8").contains(format)) {
            fetchTSByM3U8(url);
        }
    }

    private void fetchTSByM3U8(String rootUrl, String m3u8FilePath) {
        List<VideoTS> videoTSList = new ArrayList<>();

        try {
            videoTSList = m3u8Manager.getVideoTSListByFile(rootUrl, m3u8FilePath);
        } catch (IOException ioException) {
            fileReaderAndWriterMap.writeOther("list ts failed: " + m3u8FilePath);
        }

        for (VideoTS videoTS : videoTSList) {
            processUrl(videoTS.getUrl(), videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1]);
        }
    }

    private void fetchTSByM3U8(String m3u8Url) {
        List<VideoTS> videoTSList = new ArrayList<>();

        try {
            videoTSList = m3u8Manager.getVideoTSListByUrl(m3u8Url);
        } catch (IOException ioException) {
            fileReaderAndWriterMap.writeErrorOrNull("list ts failed: " + m3u8Url);
        }

        for (VideoTS videoTS : videoTSList) {
            processUrl(videoTS.getUrl(), videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1]);
        }
    }

    public void closeResource() {
        fileReaderAndWriterMap.closeWriter();
    }
}