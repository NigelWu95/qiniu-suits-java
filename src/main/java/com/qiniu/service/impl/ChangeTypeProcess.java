package com.qiniu.service.impl;

import com.google.gson.JsonObject;
import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.service.auvideo.M3U8Manager;
import com.qiniu.service.auvideo.VideoTS;
import com.qiniu.service.oss.ChangeType;
import com.qiniu.storage.Configuration;
import com.qiniu.util.DateUtils;
import com.qiniu.util.JSONConvertUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ChangeTypeProcess implements IOssFileProcess, Cloneable {

    private ChangeType changeType;
    private String bucket;
    private short fileType;
    private String resultFileDir;
    private FileReaderAndWriterMap fileReaderAndWriterMap = new FileReaderAndWriterMap();
    private M3U8Manager m3u8Manager;
    private String pointTime;
    private boolean pointTimeIsBiggerThanTimeStamp;
    private QiniuException qiniuException = null;

    public ChangeTypeProcess(QiniuAuth auth, Configuration configuration, String bucket, short fileType, String resultFileDir,
                             String pointTime, boolean pointTimeIsBiggerThanTimeStamp) throws IOException {
        this.changeType = new ChangeType(auth, configuration);
        this.bucket = bucket;
        this.fileType = fileType;
        this.resultFileDir = resultFileDir;
        this.fileReaderAndWriterMap.initWriter(resultFileDir, "type");
        this.pointTime = pointTime;
        this.pointTimeIsBiggerThanTimeStamp = pointTimeIsBiggerThanTimeStamp;
    }

    public ChangeTypeProcess(QiniuAuth auth, Configuration configuration, String bucket, short fileType, String resultFileDir,
                             M3U8Manager m3u8Manager, String pointTime, boolean pointTimeIsBiggerThanTimeStamp) throws IOException {
        this(auth, configuration, bucket, fileType, resultFileDir, pointTime, pointTimeIsBiggerThanTimeStamp);
        this.m3u8Manager = m3u8Manager;
    }

    public ChangeTypeProcess clone() throws CloneNotSupportedException {
        ChangeTypeProcess changeTypeProcess = (ChangeTypeProcess)super.clone();
        changeTypeProcess.changeType = changeType.clone();
        changeTypeProcess.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        try {
            changeTypeProcess.fileReaderAndWriterMap.initWriter(resultFileDir, "type");
        } catch (IOException e) {
            e.printStackTrace();
            throw new CloneNotSupportedException();
        }
        return changeTypeProcess;
    }

    public QiniuException qiniuException() {
        return qiniuException;
    }

    private void changeTypeResult(String bucket, String key, short fileType, int retryCount) {
        try {
            String changeResult = changeType.run(bucket, key, fileType, retryCount);
            fileReaderAndWriterMap.writeSuccess(changeResult);
        } catch (QiniuException e) {
            if (!e.response.needRetry()) qiniuException = e;
            fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + key + "\t" + fileType + "\t" + e.error());
            e.response.close();
        }
    }

    private void batchChangeTypeResult(String bucket, String key, short fileType, int retryCount) {
        try {
            String changeResult = changeType.batchRun(bucket, key, fileType, retryCount);
            if (!StringUtils.isNullOrEmpty(changeResult)) fileReaderAndWriterMap.writeSuccess(changeResult);
        } catch (QiniuException e) {
            if (!e.response.needRetry()) qiniuException = e;
            fileReaderAndWriterMap.writeErrorOrNull(changeType.getBatchOps() + "\t" + e.error());
            e.response.close();
        }
    }

    public String[] getProcessParams(String fileInfoStr, int retryCount) {

        JsonObject fileInfo = JSONConvertUtils.toJson(fileInfoStr);
        Long putTime = fileInfo.get("putTime").getAsLong();
        String key = fileInfo.get("key").getAsString();
        short type = fileInfo.get("type").getAsShort();

        boolean isDoProcess = false;
        if (StringUtils.isNullOrEmpty(pointTime)) {
            isDoProcess = true;
        } else {
            try {
                String timeString = String.valueOf(putTime);
                // 相较于时间节点的记录进行处理，并保存请求状态码和 id 到文件中。
                isDoProcess = DateUtils.compareTimeToBreakpoint(pointTime, pointTimeIsBiggerThanTimeStamp, Long.valueOf(timeString.substring(0, timeString.length() - 4)));
            } catch (Exception ex) {
                fileReaderAndWriterMap.writeErrorOrNull(key + "\t" + putTime + "\t" + type + "\t" + "date error");
            }
        }

        String[] params = new String[]{"false", key, key + "\t" + type + "\t" + isDoProcess};
        if (isDoProcess && type != fileType) params[0] = "true";
        return params;
    }

    public void processFile(String fileInfoStr, int retryCount) {
//        String[] params = getProcessParams(fileInfoStr, retryCount);
//        if ("true".equals(params[0]))
//            changeTypeResult(bucket, params[1], fileType, retryCount);
//        else
//            fileReaderAndWriterMap.writeOther(params[2]);
//    }
//
//    public void batchProcessFile(String fileInfoStr, int retryCount) {
        String[] params = getProcessParams(fileInfoStr, retryCount);
        if ("true".equals(params[0]))
            batchChangeTypeResult(bucket, params[1], fileType, retryCount);
        else
            fileReaderAndWriterMap.writeOther(params[2]);
    }

    private void changeTSByM3U8(String rootUrl, String key, int retryCount) {
        List<VideoTS> videoTSList = new ArrayList<>();

        try {
            videoTSList = m3u8Manager.getVideoTSListByFile(rootUrl, key);
        } catch (IOException ioException) {
            fileReaderAndWriterMap.writeOther("list ts failed: " + key);
        }

        for (VideoTS videoTS : videoTSList) {
            changeTypeResult(bucket, videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1], fileType, retryCount);
        }
    }

    public void closeResource() {
        fileReaderAndWriterMap.closeWriter();
        if (changeType != null)
            changeType.closeBucketManager();
    }
}