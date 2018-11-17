package com.qiniu.service.impl;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.service.oss.UpdateLifecycle;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class UpdateLifecycleProcess implements IOssFileProcess, Cloneable {

    private UpdateLifecycle updateLifecycle;
    private String bucket;
    private int days;
    private String resultFileDir;
    private String processName;
    private FileReaderAndWriterMap fileReaderAndWriterMap = new FileReaderAndWriterMap();
    private QiniuException qiniuException = null;

    public UpdateLifecycleProcess(Auth auth, Configuration configuration, String bucket, int days, String resultFileDir,
                                  String processName, int resultFileIndex) throws IOException {
        this.updateLifecycle = new UpdateLifecycle(auth, configuration);
        this.bucket = bucket;
        this.days = days;
        this.resultFileDir = resultFileDir;
        this.processName = processName;
        this.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public UpdateLifecycleProcess(Auth auth, Configuration configuration, String bucket, int days, String resultFileDir,
                                  String processName) throws IOException {
        this(auth, configuration, bucket, days, resultFileDir, processName, 0);
    }

    public UpdateLifecycleProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        UpdateLifecycleProcess updateLifecycleProcess = (UpdateLifecycleProcess)super.clone();
        updateLifecycleProcess.updateLifecycle = updateLifecycle.clone();
        updateLifecycleProcess.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        updateLifecycleProcess.qiniuException = null;
        try {
            updateLifecycleProcess.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            e.printStackTrace();
            throw new CloneNotSupportedException();
        }
        return updateLifecycleProcess;
    }

    public String getProcessName() {
        return this.processName;
    }

    public QiniuException qiniuException() {
        return qiniuException;
    }

    public void processFile(String fileKey, int retryCount) {
        try {
            String result = updateLifecycle.run(bucket, fileKey, days, retryCount);
            if (!StringUtils.isNullOrEmpty(result)) fileReaderAndWriterMap.writeSuccess(result);
        } catch (QiniuException e) {
            fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + fileKey + "\t" + days + "\t" + e.error());
            if (!e.response.needRetry()) qiniuException = e;
            else e.response.close();
        }
    }

    public void processFile(List<FileInfo> fileInfoList, int retryCount) {

        if (fileInfoList == null || fileInfoList.size() == 0) return;
        int times = fileInfoList.size()/1000 + 1;
        List<String> keyList = fileInfoList.stream().map(fileInfo -> fileInfo.key).collect(Collectors.toList());
        for (int i = 0; i < times; i++) {
            List<String> processList = keyList.subList(1000 * i, i == times - 1 ? keyList.size() : 1000 * (i + 1));
            if (processList.size() > 0) {
                try {
                    String result = updateLifecycle.batchRun(bucket, days, processList, retryCount);
                    if (!StringUtils.isNullOrEmpty(result)) fileReaderAndWriterMap.writeSuccess(result);
                } catch (QiniuException e) {
                    fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + days + "\t" + processList + "\t"
                            + e.error());
                    if (!e.response.needRetry()) qiniuException = e;
                    else e.response.close();
                }
            }
        }
    }

    public void closeResource() {
        fileReaderAndWriterMap.closeWriter();
    }
}
