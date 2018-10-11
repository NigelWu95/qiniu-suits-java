package com.qiniu.service.impl;

import com.google.gson.JsonObject;
import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.interfaces.IUrlItemProcess;
import com.qiniu.service.auvideo.M3U8Manager;
import com.qiniu.service.auvideo.VideoTS;
import com.qiniu.service.oss.BucketCopy;
import com.qiniu.storage.Configuration;
import com.qiniu.util.JSONConvertUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BucketCopyProcess implements IUrlItemProcess, IOssFileProcess, Cloneable {

    private BucketCopy bucketCopy;
    private String resultFileDir;
    private FileReaderAndWriterMap fileReaderAndWriterMap = new FileReaderAndWriterMap();
    private String srcBucket;
    private String tarBucket;
    private String keyPrefix;
    private M3U8Manager m3u8Manager;
    private QiniuException qiniuException = null;

    public BucketCopyProcess(QiniuAuth auth, Configuration configuration, String sourceBucket, String targetBucket,
                             String keyPrefix, String resultFileDir) throws IOException {
        this.bucketCopy = new BucketCopy(auth, configuration, sourceBucket, targetBucket);
        this.resultFileDir = resultFileDir;
        this.fileReaderAndWriterMap.initWriter(resultFileDir, "copy");
        this.srcBucket = sourceBucket;
        this.tarBucket = targetBucket;
        this.keyPrefix = StringUtils.isNullOrEmpty(keyPrefix) ? "" : keyPrefix;
    }

    public BucketCopyProcess(QiniuAuth auth, Configuration configuration, String sourceBucket, String targetBucket,
                             String keyPrefix, String resultFileDir, M3U8Manager m3u8Manager) throws IOException {
        this(auth, configuration, sourceBucket, targetBucket, keyPrefix, resultFileDir);
        this.m3u8Manager = m3u8Manager;
    }

    public BucketCopyProcess clone() throws CloneNotSupportedException {
        BucketCopyProcess bucketCopyProcess = (BucketCopyProcess)super.clone();
        bucketCopyProcess.bucketCopy = bucketCopy.clone();
        bucketCopyProcess.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        try {
            bucketCopyProcess.fileReaderAndWriterMap.initWriter(resultFileDir, "copy");
        } catch (IOException e) {
            e.printStackTrace();
            throw new CloneNotSupportedException();
        }
        return bucketCopyProcess;
    }

    public QiniuException qiniuException() {
        return qiniuException;
    }

    public void bucketCopyResult(String sourceBucket, String srcKey, String targetBucket, String tarKey, boolean force, int retryCount) {

        try {
            String bucketCopyResult = bucketCopy.run(sourceBucket, srcKey, targetBucket, tarKey, force, retryCount);
            fileReaderAndWriterMap.writeSuccess(bucketCopyResult);
        } catch (QiniuException e) {
            if (!e.response.needRetry()) qiniuException = e;
            fileReaderAndWriterMap.writeErrorOrNull(sourceBucket + "\t" + srcKey + "\t" + targetBucket + "\t" + tarKey + "\t" + e.error());
            e.response.close();
        }
    }

    public void processItem(String source, String item) {
        bucketCopyResult(source, item, tarBucket, this.keyPrefix + item, false, 0);
    }

    public void processItem(String source, String item, String key) {
        bucketCopyResult(source, item, tarBucket, this.keyPrefix + key, false, 0);
    }

    public void processItem(QiniuAuth auth, String source, String item) {
        bucketCopyResult(source, item, tarBucket, this.keyPrefix + item, false, 0);
    }

    public void processItem(QiniuAuth auth, String source, String item, String key) {
        bucketCopyResult(source, item, tarBucket, this.keyPrefix + key, false, 0);
    }

    public void processUrl(String url, String key) {
        bucketCopyResult(srcBucket, url.split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1], tarBucket,
                this.keyPrefix + key, false, 0);
    }

    public void processUrl(String url, String key, String format) {
        processUrl(url, key);

        if (Arrays.asList("hls", "HLS", "m3u8", "M3U8").contains(format)) {
            copyTSByM3U8(url);
        }
    }

    public void processUrl(QiniuAuth auth, String url, String key) {
        processUrl(url, key);
    }

    public void processUrl(QiniuAuth auth, String url, String key, String format) {
        processUrl(url, key, format);
    }

    private void copyTSByM3U8(String rootUrl, String m3u8FilePath) {
        List<VideoTS> videoTSList = new ArrayList<>();

        try {
            videoTSList = m3u8Manager.getVideoTSListByFile(rootUrl, m3u8FilePath);
        } catch (IOException ioException) {
            fileReaderAndWriterMap.writeOther("list ts failed: " + m3u8FilePath);
        }

        for (VideoTS videoTS : videoTSList) {
            bucketCopyResult(srcBucket, videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1],
                    tarBucket, videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1], false, 0);
        }
    }

    private void copyTSByM3U8(String m3u8Url) {
        List<VideoTS> videoTSList = new ArrayList<>();

        try {
            videoTSList = m3u8Manager.getVideoTSListByUrl(m3u8Url);
        } catch (IOException ioException) {
            fileReaderAndWriterMap.writeOther("list ts failed: " + m3u8Url);
        }

        for (VideoTS videoTS : videoTSList) {
            bucketCopyResult(srcBucket, videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1],
                    tarBucket, videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1], false, 0);
        }
    }

    public void processFile(String fileInfoStr, int retryCount) {
        JsonObject fileInfo = JSONConvertUtils.toJson(fileInfoStr);
        String key = fileInfo.get("key").getAsString();
        bucketCopyResult(srcBucket, key, tarBucket, key, false, retryCount);
    }

    public void closeResource() {
        fileReaderAndWriterMap.closeWriter();
        if (bucketCopy != null)
            bucketCopy.closeBucketManager();
    }
}