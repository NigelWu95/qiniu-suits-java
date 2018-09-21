package com.qiniu.service.impl;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuSuitsException;
import com.qiniu.http.Client;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.service.auvideo.M3U8Manager;
import com.qiniu.service.auvideo.VideoTS;
import com.qiniu.service.oss.ChangeStatusProcessor;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.DateUtil;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ChangeFileStatusProcess implements IOssFileProcess {

    private ChangeStatusProcessor changeStatusProcessor;
    private String bucket;
    private short fileStatus;
    private FileReaderAndWriterMap targetFileReaderAndWriterMap;
    private M3U8Manager m3u8Manager;
    private String pointTime;
    private boolean pointTimeIsBiggerThanTimeStamp;

    public ChangeFileStatusProcess(QiniuAuth auth, String bucket, short fileStatus,
                                   FileReaderAndWriterMap targetFileReaderAndWriterMap) throws QiniuSuitsException {
        this.changeStatusProcessor = ChangeStatusProcessor.getChangeStatusProcessor(auth, new Client());
        this.bucket = bucket;
        this.fileStatus = fileStatus;
        this.targetFileReaderAndWriterMap = targetFileReaderAndWriterMap;
    }

    public ChangeFileStatusProcess(QiniuAuth auth, String bucket, short fileStatus,
                                   FileReaderAndWriterMap targetFileReaderAndWriterMap, String pointTime,
                                   boolean pointTimeIsBiggerThanTimeStamp) throws QiniuSuitsException {
        this(auth, bucket, fileStatus, targetFileReaderAndWriterMap);
        this.pointTime = pointTime;
        this.pointTimeIsBiggerThanTimeStamp = pointTimeIsBiggerThanTimeStamp;
    }

    public ChangeFileStatusProcess(QiniuAuth auth, String bucket, short fileStatus,
                                   FileReaderAndWriterMap targetFileReaderAndWriterMap, M3U8Manager m3u8Manager) throws QiniuSuitsException {
        this(auth, bucket, fileStatus, targetFileReaderAndWriterMap);
        this.m3u8Manager = m3u8Manager;
    }

    public ChangeFileStatusProcess(QiniuAuth auth, String bucket, short fileStatus,
                                   FileReaderAndWriterMap targetFileReaderAndWriterMap, M3U8Manager m3u8Manager,
                                   String pointTime, boolean pointTimeIsBiggerThanTimeStamp) throws QiniuSuitsException {
        this(auth, bucket, fileStatus, targetFileReaderAndWriterMap);
        this.m3u8Manager = m3u8Manager;
        this.pointTime = pointTime;
        this.pointTimeIsBiggerThanTimeStamp = pointTimeIsBiggerThanTimeStamp;
    }

    private void changeStatusResult(String bucket, String key, short status) {
        try {
            String bucketCopyResult = changeStatusProcessor.doStatusChange(bucket, key, status);
            targetFileReaderAndWriterMap.writeSuccess(bucketCopyResult);
        } catch (QiniuSuitsException e) {
            targetFileReaderAndWriterMap.writeErrorAndNull(e.toString() + "\t" + bucket + "\t" + key + "\t" + status);
        }
    }

    public void processFile(FileInfo fileInfo) {
        boolean isDoProcess = false;
        try {
            String timeString = String.valueOf(fileInfo.putTime);
            // 相较于时间节点的记录进行处理，并保存请求状态码和 id 到文件中。
            isDoProcess = DateUtil.compareTimeToBreakpoint(pointTime, pointTimeIsBiggerThanTimeStamp, Long.valueOf(timeString.substring(0, timeString.length() - 4)));
        } catch (Exception ex) {
            targetFileReaderAndWriterMap.writeErrorAndNull("date error:" + fileInfo.key + "\t" + fileInfo.putTime + "\t" + fileInfo.type);
        }

        if (StringUtils.isNullOrEmpty(pointTime) || isDoProcess)
            changeStatusResult(bucket, fileInfo.key, fileStatus);
    }

    public void processFile(String rootUrl, String format, FileInfo fileInfo) {
        changeStatusResult(bucket, fileInfo.key, fileStatus);

        if (Arrays.asList("hls", "HLS", "m3u8", "M3U8").contains(format)) {
            changeTSByM3U8(rootUrl, fileInfo.key);
        }
    }

    private void changeTSByM3U8(String rootUrl, String key) {
        List<VideoTS> videoTSList = new ArrayList<>();

        try {
            videoTSList = m3u8Manager.getVideoTSListByFile(rootUrl, key);
        } catch (IOException ioException) {
            targetFileReaderAndWriterMap.writeOther("list ts failed: " + key);
        }

        for (VideoTS videoTS : videoTSList) {
            changeStatusResult(bucket, videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1], fileStatus);
        }
    }

    public void closeResource() {

    }
}