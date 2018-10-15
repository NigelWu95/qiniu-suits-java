package com.qiniu.service.impl;

import com.google.gson.JsonObject;
import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.service.oss.UpdateLifecycle;
import com.qiniu.storage.Configuration;
import com.qiniu.util.DateUtils;
import com.qiniu.util.JsonConvertUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;

public class UpdateLifecycleProcess implements IOssFileProcess, Cloneable {

    private UpdateLifecycle updateLifecycle;
    private String bucket;
    private int days;
    private String resultFileDir;
    private FileReaderAndWriterMap fileReaderAndWriterMap = new FileReaderAndWriterMap();
    private String pointTime;
    private boolean pointTimeIsBiggerThanTimeStamp;
    private QiniuException qiniuException = null;

    public UpdateLifecycleProcess(QiniuAuth auth, Configuration configuration, String bucket, int days, String resultFileDir,
                                  String pointTime, boolean pointTimeIsBiggerThanTimeStamp) throws IOException {
        this.updateLifecycle = new UpdateLifecycle(auth, configuration);
        this.bucket = bucket;
        this.days = days;
        this.resultFileDir = resultFileDir;
        this.fileReaderAndWriterMap.initWriter(resultFileDir, "lifecycle");
        this.pointTime = pointTime;
        this.pointTimeIsBiggerThanTimeStamp = pointTimeIsBiggerThanTimeStamp;
    }

    public UpdateLifecycleProcess clone() throws CloneNotSupportedException {
        UpdateLifecycleProcess updateLifecycleProcess = (UpdateLifecycleProcess)super.clone();
        updateLifecycleProcess.updateLifecycle = updateLifecycle.clone();
        updateLifecycleProcess.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        try {
            updateLifecycleProcess.fileReaderAndWriterMap.initWriter(resultFileDir, "lifecycle");
        } catch (IOException e) {
            e.printStackTrace();
            throw new CloneNotSupportedException();
        }
        return updateLifecycleProcess;
    }

    public QiniuException qiniuException() {
        return qiniuException;
    }

    private void updateLifecycleResult(String bucket, String key, int days, int retryCount, boolean batch) {
        try {
            String result = batch ?
                    updateLifecycle.batchRun(bucket, key, days, retryCount) :
                    updateLifecycle.run(bucket, key, days, retryCount);
            if (result != null) fileReaderAndWriterMap.writeSuccess(result);
        } catch (QiniuException e) {
            if (!e.response.needRetry()) qiniuException = e;
            if (batch) fileReaderAndWriterMap.writeErrorOrNull(updateLifecycle.getBatchOps() + "\t" + e.error());
            else fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + key + "\t" + days + "\t" + e.error());
            e.response.close();
        }
    }

    public String[] getProcessParams(String fileInfoStr) {

        JsonObject fileInfo = JsonConvertUtils.toJsonObject(fileInfoStr);
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

        String[] params = new String[]{"false", key, key + "\t" + days + "\t" + isDoProcess};
        if (isDoProcess) params[0] = "true";
        return params;
    }

    public void processFile(String fileInfoStr, int retryCount, boolean batch) {
        String[] params = getProcessParams(fileInfoStr);
        if ("true".equals(params[0]))
            updateLifecycleResult(bucket, params[1], days, retryCount, batch);
        else
            fileReaderAndWriterMap.writeOther(params[2]);
    }

    public void checkBatchProcess(int retryCount) {
        try {
            String result = updateLifecycle.batchCheckRun(retryCount);
            if (result != null) fileReaderAndWriterMap.writeSuccess(result);
        } catch (QiniuException e) {
            if (!e.response.needRetry()) qiniuException = e;
            fileReaderAndWriterMap.writeErrorOrNull(updateLifecycle.getBatchOps() + "\t" + e.error());
            e.response.close();
        }
    }

    public void closeResource() {
        fileReaderAndWriterMap.closeWriter();
        if (updateLifecycle != null)
            updateLifecycle.closeBucketManager();
    }
}