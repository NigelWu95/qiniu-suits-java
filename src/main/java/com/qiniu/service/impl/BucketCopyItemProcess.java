package com.qiniu.service.impl;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuBucketManager;
import com.qiniu.common.QiniuSuitsException;
import com.qiniu.interfaces.IUrlItemProcess;
import com.qiniu.service.auvideo.M3U8Manager;
import com.qiniu.service.auvideo.VideoTS;
import com.qiniu.service.oss.BucketCopyProcessor;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BucketCopyItemProcess implements IUrlItemProcess {

    private BucketCopyProcessor bucketCopyProcessor;
    private FileReaderAndWriterMap targetFileReaderAndWriterMap;
    private M3U8Manager m3u8Manager;
    private String keyPrefix;

    public BucketCopyItemProcess(QiniuBucketManager bucketManager, String sourceBucket, String targetBucket, String keyPrefix, FileReaderAndWriterMap targetFileReaderAndWriterMap) throws QiniuSuitsException {
        this.bucketCopyProcessor = BucketCopyProcessor.getBucketCopyProcessor(bucketManager, sourceBucket, targetBucket);
        this.targetFileReaderAndWriterMap = targetFileReaderAndWriterMap;
        this.keyPrefix = StringUtils.isNullOrEmpty(keyPrefix) ? "" : keyPrefix;
    }

    public BucketCopyItemProcess(QiniuBucketManager bucketManager, String sourceBucket, String targetBucket, String keyPrefix, FileReaderAndWriterMap targetFileReaderAndWriterMap, M3U8Manager m3u8Manager) throws QiniuSuitsException {
        this.bucketCopyProcessor = BucketCopyProcessor.getBucketCopyProcessor(bucketManager, sourceBucket, targetBucket);
        this.targetFileReaderAndWriterMap = targetFileReaderAndWriterMap;
        this.m3u8Manager = m3u8Manager;
        this.keyPrefix = StringUtils.isNullOrEmpty(keyPrefix) ? "" : keyPrefix;
    }

    private void bucketCopyResult(String srcBucket, String srcKey, String tarKey) {
        try {
            String bucketCopyResult = bucketCopyProcessor.doBucketCopy(srcBucket, srcKey, tarKey);
            targetFileReaderAndWriterMap.writeSuccess(bucketCopyResult);
        } catch (QiniuSuitsException e) {
            targetFileReaderAndWriterMap.writeErrorAndNull(e.toString() + "\t" + srcBucket + "\t" + srcKey + "\t" + tarKey);
        }
    }

    public void processItem(String source, String item) {
        bucketCopyResult(source, item, this.keyPrefix + item);
    }

    public void processItem(String source, String item, String key) {
        bucketCopyResult(source, item, this.keyPrefix + key);
    }

    public void processItem(QiniuAuth auth, String source, String item) {
        bucketCopyResult(source, item, this.keyPrefix + item);
    }

    public void processItem(QiniuAuth auth, String source, String item, String key) {
        bucketCopyResult(source, item, this.keyPrefix + key);
    }

    public void processUrl(String url, String key) {
        bucketCopyResult(null, url.split("(https?://(\\S+\\.){1,5}\\S+/)|(\\?ver=)")[1], this.keyPrefix + key);
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

    public void copyTSByM3U8(String rootUrl, String m3u8FilePath) {
        List<VideoTS> videoTSList = new ArrayList<>();

        try {
            videoTSList = m3u8Manager.getVideoTSListByFile(rootUrl, m3u8FilePath);
        } catch (IOException ioException) {
            targetFileReaderAndWriterMap.writeOther("list ts failed: " + m3u8FilePath);
        }

        for (VideoTS videoTS : videoTSList) {
            bucketCopyResult(null, videoTS.getUrl().split("(https?://(\\S+\\.){1,5}\\S+/)|(\\?ver=)")[1], videoTS.getUrl().split("(https?://(\\S+\\.){1,5}\\S+/)|(\\?ver=)")[1]);
        }
    }

    public void copyTSByM3U8(String m3u8Url) {
        List<VideoTS> videoTSList = new ArrayList<>();

        try {
            videoTSList = m3u8Manager.getVideoTSListByUrl(m3u8Url);
        } catch (IOException ioException) {
            targetFileReaderAndWriterMap.writeOther("list ts failed: " + m3u8Url);
        }

        for (VideoTS videoTS : videoTSList) {
            bucketCopyResult(null, videoTS.getUrl().split("(https?://(\\S+\\.){1,5}\\S+/)|(\\?ver=)")[1], videoTS.getUrl().split("(https?://(\\S+\\.){1,5}\\S+/)|(\\?ver=)")[1]);
        }
    }

    public void close() {
        if (bucketCopyProcessor != null) {
            bucketCopyProcessor.closeClient();
        }
    }
}