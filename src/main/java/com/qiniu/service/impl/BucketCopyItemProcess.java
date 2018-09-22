package com.qiniu.service.impl;

import com.google.gson.JsonObject;
import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuSuitsException;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.interfaces.IUrlItemProcess;
import com.qiniu.service.auvideo.M3U8Manager;
import com.qiniu.service.auvideo.VideoTS;
import com.qiniu.service.oss.BucketCopyProcessor;
import com.qiniu.storage.Configuration;
import com.qiniu.util.JSONConvertUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BucketCopyItemProcess implements IUrlItemProcess, IOssFileProcess {

    private BucketCopyProcessor bucketCopyProcessor;
    private FileReaderAndWriterMap targetFileReaderAndWriterMap;
    private String srcBucket;
    private String tarBucket;
    private String keyPrefix;
    private M3U8Manager m3u8Manager;

    public BucketCopyItemProcess(QiniuAuth auth, Configuration configuration, String sourceBucket, String targetBucket, String keyPrefix,
                                 FileReaderAndWriterMap targetFileReaderAndWriterMap) throws QiniuSuitsException {
        this.bucketCopyProcessor = BucketCopyProcessor.getBucketCopyProcessor(auth, configuration, sourceBucket, targetBucket);
        this.targetFileReaderAndWriterMap = targetFileReaderAndWriterMap;
        this.srcBucket = sourceBucket;
        this.tarBucket = targetBucket;
        this.keyPrefix = StringUtils.isNullOrEmpty(keyPrefix) ? "" : keyPrefix;
    }

    public BucketCopyItemProcess(QiniuAuth auth, Configuration configuration, String sourceBucket, String targetBucket, String keyPrefix,
                                 FileReaderAndWriterMap targetFileReaderAndWriterMap, M3U8Manager m3u8Manager) throws QiniuSuitsException {
        this(auth, configuration, sourceBucket, targetBucket, keyPrefix, targetFileReaderAndWriterMap);
        this.m3u8Manager = m3u8Manager;
    }

    private void bucketCopyResult(String sourceBucket, String srcKey, String targetBucket, String tarKey) {
        try {
            String bucketCopyResult = bucketCopyProcessor.doBucketCopy(sourceBucket, srcKey, targetBucket, tarKey);
            targetFileReaderAndWriterMap.writeSuccess(bucketCopyResult);
        } catch (QiniuSuitsException e) {
            targetFileReaderAndWriterMap.writeErrorAndNull(e.toString() + "\t" + sourceBucket + "\t" + srcKey + "\t"
                    + targetBucket + "\t" + tarKey);
        }
    }

    public void processItem(String source, String item) {
        bucketCopyResult(source, item, tarBucket, this.keyPrefix + item);
    }

    public void processItem(String source, String item, String key) {
        bucketCopyResult(source, item, tarBucket, this.keyPrefix + key);
    }

    public void processItem(QiniuAuth auth, String source, String item) {
        bucketCopyResult(source, item, tarBucket, this.keyPrefix + item);
    }

    public void processItem(QiniuAuth auth, String source, String item, String key) {
        bucketCopyResult(source, item, tarBucket, this.keyPrefix + key);
    }

    public void processUrl(String url, String key) {
        bucketCopyResult(srcBucket, url.split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1], tarBucket,
                this.keyPrefix + key);
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
            targetFileReaderAndWriterMap.writeOther("list ts failed: " + m3u8FilePath);
        }

        for (VideoTS videoTS : videoTSList) {
            bucketCopyResult(srcBucket, videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1],
                    tarBucket, videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1]);
        }
    }

    private void copyTSByM3U8(String m3u8Url) {
        List<VideoTS> videoTSList = new ArrayList<>();

        try {
            videoTSList = m3u8Manager.getVideoTSListByUrl(m3u8Url);
        } catch (IOException ioException) {
            targetFileReaderAndWriterMap.writeOther("list ts failed: " + m3u8Url);
        }

        for (VideoTS videoTS : videoTSList) {
            bucketCopyResult(srcBucket, videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1],
                    tarBucket, videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1]);
        }
    }

    public void processFile(String fileInfoStr) {
        JsonObject fileInfo = JSONConvertUtils.toJson(fileInfoStr);
        String key = fileInfo.get("key").getAsString();
        bucketCopyResult(srcBucket, key, tarBucket, key);
    }

    public void closeResource() {
        if (bucketCopyProcessor != null)
            bucketCopyProcessor.closeBucketManager();
    }
}