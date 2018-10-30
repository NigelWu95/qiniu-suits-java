package com.qiniu.service.impl;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.service.oss.UpdateLifecycle;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.List;

public class UpdateLifecycleProcess implements IOssFileProcess {

    private UpdateLifecycle updateLifecycle;
    private String bucket;
    private int days;
    private String resultFileDir;
    private FileReaderAndWriterMap fileReaderAndWriterMap = new FileReaderAndWriterMap();
    private QiniuException qiniuException = null;

    public UpdateLifecycleProcess(Auth auth, Configuration configuration, String bucket, int days, String resultFileDir,
                                  int resultFileIndex) throws IOException {
        this.updateLifecycle = new UpdateLifecycle(auth, configuration);
        this.bucket = bucket;
        this.days = days;
        this.resultFileDir = resultFileDir;
        this.fileReaderAndWriterMap.initWriter(resultFileDir, "lifecycle", resultFileIndex);
    }

    public UpdateLifecycleProcess(Auth auth, Configuration configuration, String bucket, int days, String resultFileDir)
            throws IOException {
        this(auth, configuration, bucket, days, resultFileDir, 0);
    }

    public UpdateLifecycleProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        UpdateLifecycleProcess updateLifecycleProcess = (UpdateLifecycleProcess)super.clone();
        updateLifecycleProcess.updateLifecycle = updateLifecycle.clone();
        updateLifecycleProcess.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        try {
            updateLifecycleProcess.fileReaderAndWriterMap.initWriter(resultFileDir, "lifecycle", resultFileIndex);
        } catch (IOException e) {
            e.printStackTrace();
            throw new CloneNotSupportedException();
        }
        return updateLifecycleProcess;
    }

    public QiniuException qiniuException() {
        return qiniuException;
    }

    public void processFile(String fileKey, int retryCount) {
        try {
            String result = updateLifecycle.run(bucket, fileKey, days, retryCount);
            if (!StringUtils.isNullOrEmpty(result)) fileReaderAndWriterMap.writeSuccess(result);
        } catch (QiniuException e) {
            if (!e.response.needRetry()) qiniuException = e;
            fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + fileKey + "\t" + days + "\t" + e.error());
            e.response.close();
        }
    }

    public void processFile(List<String> keyList, int retryCount) {

        if (keyList == null || keyList.size() == 0) return;
        int times = keyList.size()/1000 + 1;
        for (int i = 0; i < times; i++) {
            List<String> processList = keyList.subList(1000 * i, i == times - 1 ? keyList.size() : 1000 * (i + 1));
            if (processList.size() > 0) {
                try {
                    String result = updateLifecycle.batchRun(bucket, processList, days, retryCount);
                    if (!StringUtils.isNullOrEmpty(result)) fileReaderAndWriterMap.writeSuccess(result);
                } catch (QiniuException e) {
                    if (!e.response.needRetry()) qiniuException = e;
                    fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + processList + "\t" + days + "\t" + e.error());
                    e.response.close();
                }
            }
        }
    }

    public void closeResource() {
        fileReaderAndWriterMap.closeWriter();
    }
}