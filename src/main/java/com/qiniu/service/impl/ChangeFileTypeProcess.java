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
import com.qiniu.util.DateUtil;
import com.qiniu.util.JSONConvertUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ChangeFileTypeProcess implements IOssFileProcess {

    private ChangeFileTypeProcessor changeFileTypeProcessor;
    private String bucket;
    private StorageType fileType;
    private FileReaderAndWriterMap targetFileReaderAndWriterMap;
    private M3U8Manager m3u8Manager;
    private String pointTime;
    private boolean pointTimeIsBiggerThanTimeStamp;

    public ChangeFileTypeProcess(QiniuAuth auth, Configuration configuration, String bucket, QiniuBucketManager.StorageType fileType,
                                 FileReaderAndWriterMap targetFileReaderAndWriterMap) {
        this.changeFileTypeProcessor = ChangeFileTypeProcessor.getChangeFileTypeProcessor(auth, configuration);
        this.bucket = bucket;
        this.fileType = fileType;
        this.targetFileReaderAndWriterMap = targetFileReaderAndWriterMap;
    }

    public ChangeFileTypeProcess(QiniuAuth auth, Configuration configuration, String bucket, StorageType fileType,
                                 FileReaderAndWriterMap targetFileReaderAndWriterMap, String pointTime,
                                 boolean pointTimeIsBiggerThanTimeStamp) {
        this(auth, configuration, bucket, fileType, targetFileReaderAndWriterMap);
        this.pointTime = pointTime;
        this.pointTimeIsBiggerThanTimeStamp = pointTimeIsBiggerThanTimeStamp;
    }

    public ChangeFileTypeProcess(QiniuAuth auth, Configuration configuration, String bucket, StorageType fileType,
                                 FileReaderAndWriterMap targetFileReaderAndWriterMap, M3U8Manager m3u8Manager) {
        this(auth, configuration, bucket, fileType, targetFileReaderAndWriterMap);
        this.m3u8Manager = m3u8Manager;
    }

    public ChangeFileTypeProcess(QiniuAuth auth, Configuration configuration, String bucket, StorageType fileType,
                                 FileReaderAndWriterMap targetFileReaderAndWriterMap, M3U8Manager m3u8Manager,
                                 String pointTime, boolean pointTimeIsBiggerThanTimeStamp) {
        this(auth, configuration, bucket, fileType, targetFileReaderAndWriterMap);
        this.m3u8Manager = m3u8Manager;
        this.pointTime = pointTime;
        this.pointTimeIsBiggerThanTimeStamp = pointTimeIsBiggerThanTimeStamp;
    }

    private void changeStatusResult(String bucket, String key, StorageType fileType) {
        try {
            String bucketCopyResult = changeFileTypeProcessor.doFileTypeChange(bucket, key, fileType);
            targetFileReaderAndWriterMap.writeSuccess(bucketCopyResult);
        } catch (QiniuSuitsException e) {
            targetFileReaderAndWriterMap.writeErrorAndNull(e.toString() + "\t" + bucket + "\t" + key + "\t" + fileType);
        }
    }

    public void processFile(String fileInfoStr) {
        JsonObject fileInfo = JSONConvertUtils.toJson(fileInfoStr);
        Long putTime = fileInfo.get("putTime").getAsLong();
        String key = fileInfo.get("key").getAsString();
        int type = fileInfo.get("type").getAsInt();
        if (type == fileType.ordinal()) {
            targetFileReaderAndWriterMap.writeOther("file " + key + " type originally is " + type);
            return;
        }
        boolean isDoProcess = false;
        try {
            String timeString = String.valueOf(putTime);
            // 相较于时间节点的记录进行处理，并保存请求状态码和 id 到文件中。
            isDoProcess = DateUtil.compareTimeToBreakpoint(pointTime, pointTimeIsBiggerThanTimeStamp, Long.valueOf(timeString.substring(0, timeString.length() - 4)));
        } catch (Exception ex) {
            targetFileReaderAndWriterMap.writeErrorAndNull("date error:" + key + "\t" + putTime + "\t" + type);
        }

        if (StringUtils.isNullOrEmpty(pointTime) || isDoProcess)
            changeStatusResult(bucket, key, fileType);
    }

    private void changeTSByM3U8(String rootUrl, String key) {
        List<VideoTS> videoTSList = new ArrayList<>();

        try {
            videoTSList = m3u8Manager.getVideoTSListByFile(rootUrl, key);
        } catch (IOException ioException) {
            targetFileReaderAndWriterMap.writeOther("list ts failed: " + key);
        }

        for (VideoTS videoTS : videoTSList) {
            changeStatusResult(bucket, videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1], fileType);
        }
    }

    public void closeResource() {
        if (changeFileTypeProcessor != null)
            changeFileTypeProcessor.closeBucketManager();
    }
}