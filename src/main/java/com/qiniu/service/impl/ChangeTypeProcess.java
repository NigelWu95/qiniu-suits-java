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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ChangeTypeProcess implements IOssFileProcess {

    private ChangeType changeType;
    private String bucket;
    private short fileType;
    private FileReaderAndWriterMap fileReaderAndWriterMap = new FileReaderAndWriterMap();
    private M3U8Manager m3u8Manager;
    private String pointTime;
    private boolean pointTimeIsBiggerThanTimeStamp;
    private QiniuException qiniuException = null;
    private ExecutorService executorPool;

    public ChangeTypeProcess(QiniuAuth auth, Configuration configuration, String bucket, short fileType, String resultFileDir,
                             String pointTime, boolean pointTimeIsBiggerThanTimeStamp) throws IOException {
        this.changeType = ChangeType.getInstance(auth, configuration);
        this.bucket = bucket;
        this.fileType = fileType;
        this.fileReaderAndWriterMap.initWriter(resultFileDir, "type");
        this.pointTime = pointTime;
        this.pointTimeIsBiggerThanTimeStamp = pointTimeIsBiggerThanTimeStamp;
    }

    public ChangeTypeProcess(QiniuAuth auth, Configuration configuration, String bucket, short fileType, String resultFileDir,
                             String pointTime, boolean pointTimeIsBiggerThanTimeStamp, int threads) throws IOException {
        this(auth, configuration, bucket, fileType, resultFileDir, pointTime, pointTimeIsBiggerThanTimeStamp);
        this.executorPool = Executors.newFixedThreadPool(threads);
    }

    public ChangeTypeProcess(QiniuAuth auth, Configuration configuration, String bucket, short fileType, String resultFileDir,
                             M3U8Manager m3u8Manager, String pointTime, boolean pointTimeIsBiggerThanTimeStamp) throws IOException {
        this(auth, configuration, bucket, fileType, resultFileDir, pointTime, pointTimeIsBiggerThanTimeStamp);
        this.m3u8Manager = m3u8Manager;
    }

    public ChangeTypeProcess(QiniuAuth auth, Configuration configuration, String bucket, short fileType, String resultFileDir,
                             M3U8Manager m3u8Manager, String pointTime, boolean pointTimeIsBiggerThanTimeStamp, int threads) throws IOException {
        this(auth, configuration, bucket, fileType, resultFileDir, pointTime, pointTimeIsBiggerThanTimeStamp);
        this.m3u8Manager = m3u8Manager;
        this.executorPool = Executors.newFixedThreadPool(threads);
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

    public void processFile(String fileInfoStr, int retryCount) {

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

        if (isDoProcess && type != fileType)
            changeTypeResult(bucket, key, fileType, retryCount);
        else
            fileReaderAndWriterMap.writeOther(key + "\t" + type + "\t" + isDoProcess);
    }

//    public void processFile(String fileInfoStr, int retryCount) {
//
//        if (executorPool != null) {
//            executorPool.submit(() -> {
//                doProcess(fileInfoStr, retryCount);
//            });
//        } else {
//            doProcess(fileInfoStr, retryCount);
//        }
//    }

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