package com.qiniu.service.impl;

import com.google.gson.JsonObject;
import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.service.auvideo.M3U8Manager;
import com.qiniu.service.auvideo.VideoTS;
import com.qiniu.service.oss.ChangeStatus;
import com.qiniu.storage.Configuration;
import com.qiniu.util.DateUtils;
import com.qiniu.util.JSONConvertUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ChangeStatusProcess implements IOssFileProcess, Cloneable {

    private ChangeStatus changeStatus;
    private String bucket;
    private short fileStatus;
    private String resultFileDir;
    private FileReaderAndWriterMap fileReaderAndWriterMap = new FileReaderAndWriterMap();
    private String pointTime;
    private boolean pointTimeIsBiggerThanTimeStamp;
    private QiniuException qiniuException = null;

    public ChangeStatusProcess(QiniuAuth auth, Configuration configuration, String bucket, short fileStatus, String resultFileDir,
                               String pointTime, boolean pointTimeIsBiggerThanTimeStamp) throws IOException {
        this.changeStatus = new ChangeStatus(auth, configuration);
        this.bucket = bucket;
        this.fileStatus = fileStatus;
        this.resultFileDir = resultFileDir;
        this.fileReaderAndWriterMap.initWriter(resultFileDir, "status");
        this.pointTime = pointTime;
        this.pointTimeIsBiggerThanTimeStamp = pointTimeIsBiggerThanTimeStamp;
    }

    public ChangeStatusProcess clone() throws CloneNotSupportedException {
        ChangeStatusProcess changeStatusProcess = (ChangeStatusProcess)super.clone();
        changeStatusProcess.changeStatus = changeStatus.clone();
        changeStatusProcess.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        try {
            changeStatusProcess.fileReaderAndWriterMap.initWriter(resultFileDir, "status");
        } catch (IOException e) {
            e.printStackTrace();
            throw new CloneNotSupportedException();
        }
        return changeStatusProcess;
    }

    public QiniuException qiniuException() {
        return qiniuException;
    }

    private void changeStatusResult(String bucket, String key, short fileStatus, int retryCount, boolean batch) {
        try {
            String changeResult = batch ?
                    changeStatus.batchRun(bucket, key, fileStatus, retryCount) :
                    changeStatus.run(bucket, key, fileStatus, retryCount);
            if (changeResult != null) fileReaderAndWriterMap.writeSuccess(changeResult);
        } catch (QiniuException e) {
            if (!e.response.needRetry()) qiniuException = e;
            if (batch) fileReaderAndWriterMap.writeErrorOrNull(changeStatus.getBatchOps() + "\t" + e.error());
            else fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + key + "\t" + fileStatus + "\t" + e.error());
            e.response.close();
        }
    }

    public String[] getProcessParams(String fileInfoStr) {

        JsonObject fileInfo = JSONConvertUtils.toJsonObject(fileInfoStr);
        Long putTime = fileInfo.get("putTime").getAsLong();
        String key = fileInfo.get("key").getAsString();

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

        String[] params = new String[]{"false", key, key + "\t" + fileStatus + "\t" + isDoProcess};
        if (isDoProcess) params[0] = "true";
        return params;
    }

    public void processFile(String fileInfoStr, int retryCount, boolean batch) {
        String[] params = getProcessParams(fileInfoStr);
        if ("true".equals(params[0]))
            changeStatusResult(bucket, params[1], fileStatus, retryCount, batch);
        else
            fileReaderAndWriterMap.writeOther(params[2]);
    }

    public void closeResource() {
        fileReaderAndWriterMap.closeWriter();
    }
}