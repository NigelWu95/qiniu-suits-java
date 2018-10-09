package com.qiniu.service.impl;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuException;
import com.qiniu.service.auvideo.M3U8Manager;
import com.qiniu.service.auvideo.VideoTS;
import com.qiniu.interfaces.IUrlItemProcess;
import com.qiniu.service.oss.AsyncFetch;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AsyncFetchProcess implements IUrlItemProcess {

    private AsyncFetch asyncFetch;
    private FileReaderAndWriterMap fileReaderAndWriterMap = new FileReaderAndWriterMap();
    private M3U8Manager m3u8Manager;
    private QiniuException qiniuException = null;

    public AsyncFetchProcess(QiniuAuth auth, String targetBucket, String resultFileDir) throws IOException {
        this.asyncFetch = AsyncFetch.getInstance(auth, targetBucket);
        this.fileReaderAndWriterMap.initWriter(resultFileDir, "fetch");
    }

    public AsyncFetchProcess(QiniuAuth auth, String targetBucket, String resultFileDir, M3U8Manager m3u8Manager) throws IOException {
        this(auth, targetBucket, resultFileDir);
        this.m3u8Manager = m3u8Manager;
    }

    private void fetchResult(String url, String key) {
        try {
            String fetchResult = asyncFetch.run(url, key, 0);
            fileReaderAndWriterMap.writeSuccess(fetchResult);
        } catch (QiniuException e) {
            if (!e.response.needRetry()) qiniuException = e;
            fileReaderAndWriterMap.writeErrorOrNull(url + "," + key + "\t" + e.error());
            e.response.close();
        }
    }

    public QiniuException qiniuException() {
        return qiniuException;
    }

    public void processItem(String source, String item) {
        processItem(source, item, item);
    }

    public void processItem(String source, String item, String key) {
        String url = source.endsWith("/") ? source + item : source + "/" + item;
        fetchResult(url, key);
    }

    public void processItem(QiniuAuth auth, String source, String item) {
        processItem(auth, source, item, item);
    }

    public void processItem(QiniuAuth auth, String source, String item, String key) {
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

    public void processUrl(QiniuAuth auth, String url, String key) {
        url = auth.privateDownloadUrl(url);
        fetchResult(url, key);
    }

    public void processUrl(QiniuAuth auth, String url, String key, String format) {
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
            fileReaderAndWriterMap.writeOther("list ts failed: " + m3u8Url);
        }

        for (VideoTS videoTS : videoTSList) {
            processUrl(videoTS.getUrl(), videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1]);
        }
    }

    public void closeResource() {
        fileReaderAndWriterMap.closeWriter();
    }
}