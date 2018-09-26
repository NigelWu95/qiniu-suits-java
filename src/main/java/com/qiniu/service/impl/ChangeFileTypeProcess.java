package com.qiniu.service.impl;

import com.google.gson.JsonObject;
import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuBucketManager;
import com.qiniu.common.QiniuBucketManager.*;
import com.qiniu.common.QiniuSuitsException;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.service.auvideo.M3U8Manager;
import com.qiniu.service.auvideo.VideoTS;
import com.qiniu.service.oss.ChangeFileTypeProcessor;
import com.qiniu.storage.Configuration;
import com.qiniu.util.DateUtils;
import com.qiniu.util.JSONConvertUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ChangeFileTypeProcess implements IOssFileProcess {

    private ChangeFileTypeProcessor changeFileTypeProcessor;
    private String bucket;
    private short fileType;
    private FileReaderAndWriterMap targetFileReaderAndWriterMap;
    private M3U8Manager m3u8Manager;
    private String pointTime;
    private boolean pointTimeIsBiggerThanTimeStamp;

    public ChangeFileTypeProcess(QiniuAuth auth, Configuration configuration, String bucket, short fileType,
                                 FileReaderAndWriterMap targetFileReaderAndWriterMap) {
        this.changeFileTypeProcessor = ChangeFileTypeProcessor.getChangeFileTypeProcessor(auth, configuration);
        this.bucket = bucket;
        this.fileType = fileType;
        this.targetFileReaderAndWriterMap = targetFileReaderAndWriterMap;
    }

    public ChangeFileTypeProcess(QiniuAuth auth, Configuration configuration, String bucket, short fileType,
                                 FileReaderAndWriterMap targetFileReaderAndWriterMap, String pointTime,
                                 boolean pointTimeIsBiggerThanTimeStamp) {
        this(auth, configuration, bucket, fileType, targetFileReaderAndWriterMap);
        this.pointTime = pointTime;
        this.pointTimeIsBiggerThanTimeStamp = pointTimeIsBiggerThanTimeStamp;
    }

    public ChangeFileTypeProcess(QiniuAuth auth, Configuration configuration, String bucket, short fileType,
                                 FileReaderAndWriterMap targetFileReaderAndWriterMap, M3U8Manager m3u8Manager) {
        this(auth, configuration, bucket, fileType, targetFileReaderAndWriterMap);
        this.m3u8Manager = m3u8Manager;
    }

    public ChangeFileTypeProcess(QiniuAuth auth, Configuration configuration, String bucket, short fileType,
                                 FileReaderAndWriterMap targetFileReaderAndWriterMap, M3U8Manager m3u8Manager,
                                 String pointTime, boolean pointTimeIsBiggerThanTimeStamp) {
        this(auth, configuration, bucket, fileType, targetFileReaderAndWriterMap);
        this.m3u8Manager = m3u8Manager;
        this.pointTime = pointTime;
        this.pointTimeIsBiggerThanTimeStamp = pointTimeIsBiggerThanTimeStamp;
    }

    private void changeTypeResult(String bucket, String key, short fileType, int retryCount) {
        try {
            String bucketCopyResult = changeFileTypeProcessor.doFileTypeChange(bucket, key, fileType, retryCount);
            targetFileReaderAndWriterMap.writeSuccess(bucketCopyResult);
        } catch (QiniuSuitsException e) {
            targetFileReaderAndWriterMap.writeErrorAndNull(e.toString() + "\t" + bucket + "\t" + key + "\t" + fileType);
        }
    }

    public void processFile(String fileInfoStr) {}

    public void processFile(String fileInfoStr, int retryCount) {
        JsonObject fileInfo = JSONConvertUtils.toJson(fileInfoStr);
        Long putTime = fileInfo.get("putTime").getAsLong();
        String key = fileInfo.get("key").getAsString();
        short type = fileInfo.get("type").getAsShort();
        if (type == fileType) {
            targetFileReaderAndWriterMap.writeOther("file " + key + " type originally is " + type);
            return;
        }

        if (StringUtils.isNullOrEmpty(pointTime)) {
            changeTypeResult(bucket, key, fileType, retryCount);
        } else {
            boolean isDoProcess = false;
            try {
                String timeString = String.valueOf(putTime);
                // 相较于时间节点的记录进行处理，并保存请求状态码和 id 到文件中。
                isDoProcess = DateUtils.compareTimeToBreakpoint(pointTime, pointTimeIsBiggerThanTimeStamp, Long.valueOf(timeString.substring(0, timeString.length() - 4)));
            } catch (Exception ex) {
                targetFileReaderAndWriterMap.writeErrorAndNull("date error:" + key + "\t" + putTime + "\t" + type);
            }

            if (isDoProcess)
                changeTypeResult(bucket, key, fileType, retryCount);
        }
    }

    private void changeTSByM3U8(String rootUrl, String key, int retryCount) {
        List<VideoTS> videoTSList = new ArrayList<>();

        try {
            videoTSList = m3u8Manager.getVideoTSListByFile(rootUrl, key);
        } catch (IOException ioException) {
            targetFileReaderAndWriterMap.writeOther("list ts failed: " + key);
        }

        for (VideoTS videoTS : videoTSList) {
            changeTypeResult(bucket, videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1], fileType, retryCount);
        }
    }

    public void closeResource() {
        if (changeFileTypeProcessor != null)
            changeFileTypeProcessor.closeBucketManager();
    }
}