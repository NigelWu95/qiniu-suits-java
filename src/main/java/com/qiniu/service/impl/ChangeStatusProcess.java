package com.qiniu.service.impl;

import com.google.gson.JsonObject;
import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.service.auvideo.M3U8Manager;
import com.qiniu.service.auvideo.VideoTS;
import com.qiniu.service.oss.ChangeStatus;
import com.qiniu.util.DateUtils;
import com.qiniu.util.JSONConvertUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ChangeStatusProcess implements IOssFileProcess {

    private ChangeStatus changeStatus;
    private String bucket;
    private short fileStatus;
    private FileReaderAndWriterMap fileReaderAndWriterMap = new FileReaderAndWriterMap();
    private M3U8Manager m3u8Manager;
    private String pointTime;
    private boolean pointTimeIsBiggerThanTimeStamp;
    private QiniuException qiniuException = null;

    public ChangeStatusProcess(QiniuAuth auth, String bucket, short fileStatus, String resultFileDir) throws IOException {
        this.changeStatus = ChangeStatus.getInstance(auth, new Client());
        this.bucket = bucket;
        this.fileStatus = fileStatus;
        this.fileReaderAndWriterMap.initWriter(resultFileDir, "status");
    }

    public ChangeStatusProcess(QiniuAuth auth, String bucket, short fileStatus, String resultFileDir, String pointTime,
                               boolean pointTimeIsBiggerThanTimeStamp) throws IOException {
        this(auth, bucket, fileStatus, resultFileDir);
        this.pointTime = pointTime;
        this.pointTimeIsBiggerThanTimeStamp = pointTimeIsBiggerThanTimeStamp;
    }

    public ChangeStatusProcess(QiniuAuth auth, String bucket, short fileStatus, String resultFileDir, M3U8Manager m3u8Manager)
            throws IOException {
        this(auth, bucket, fileStatus, resultFileDir);
        this.m3u8Manager = m3u8Manager;
    }

    public ChangeStatusProcess(QiniuAuth auth, String bucket, short fileStatus, String resultFileDir, M3U8Manager m3u8Manager,
                               String pointTime, boolean pointTimeIsBiggerThanTimeStamp) throws IOException {
        this(auth, bucket, fileStatus, resultFileDir);
        this.m3u8Manager = m3u8Manager;
        this.pointTime = pointTime;
        this.pointTimeIsBiggerThanTimeStamp = pointTimeIsBiggerThanTimeStamp;
    }

    public QiniuException qiniuException() {
        return qiniuException;
    }

    private void changeStatusResult(String bucket, String key, short status, int retryCount) {
        try {
            String changeResult = changeStatus.run(bucket, key, status, retryCount);
            fileReaderAndWriterMap.writeSuccess(changeResult);
        } catch (QiniuException e) {
            if (!e.response.needRetry()) qiniuException = e;
            fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + key + "\t" + status + "\t" + e.error());
            e.response.close();
        }
    }

    public void processFile(String fileInfoStr, int retryCount) {
        JsonObject fileInfo = JSONConvertUtils.toJson(fileInfoStr);
        Long putTime = fileInfo.get("putTime").getAsLong();
        String key = fileInfo.get("key").getAsString();
        short status = fileInfo.get("status").getAsShort();

        boolean isDoProcess = false;
        if (StringUtils.isNullOrEmpty(pointTime)) {
            isDoProcess = true;
        } else {
            try {
                // 相较于时间节点的记录进行处理，并保存请求状态码和 id 到文件中。
                isDoProcess = DateUtils.compareTimeToBreakpoint(pointTime, pointTimeIsBiggerThanTimeStamp, putTime/10000);
            } catch (Exception ex) {
                fileReaderAndWriterMap.writeErrorOrNull( key + "\t" + putTime + "\t" + "date error");
            }
        }

        if (isDoProcess && status != fileStatus)
            changeStatusResult(bucket, fileInfo.get("key").getAsString(), fileStatus, retryCount);
        else
            fileReaderAndWriterMap.writeOther(key + "\t" + status + "\t" + isDoProcess);
    }

    private void changeTSByM3U8(String rootUrl, String key, int retryCount) {
        List<VideoTS> videoTSList = new ArrayList<>();

        try {
            videoTSList = m3u8Manager.getVideoTSListByFile(rootUrl, key);
        } catch (IOException ioException) {
            fileReaderAndWriterMap.writeOther("list ts failed: " + key);
        }

        for (VideoTS videoTS : videoTSList) {
            changeStatusResult(bucket, videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1], fileStatus, retryCount);
        }
    }

    public void closeResource() {
        fileReaderAndWriterMap.closeWriter();
    }
}