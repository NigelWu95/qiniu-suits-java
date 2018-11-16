package com.qiniu.service.impl;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.service.oss.ChangeStatus;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.List;

public class ChangeStatusProcess implements IOssFileProcess, Cloneable {

    private ChangeStatus changeStatus;
    private String bucket;
    private int status;
    private String resultFileDir;
    private String processName;
    private FileReaderAndWriterMap fileReaderAndWriterMap = new FileReaderAndWriterMap();
    private QiniuException qiniuException = null;

    public ChangeStatusProcess(Auth auth, Configuration configuration, String bucket, int status, String resultFileDir,
                               String processName, int resultFileIndex) throws IOException {
        this.changeStatus = new ChangeStatus(auth, configuration);
        this.bucket = bucket;
        this.status = status;
        this.resultFileDir = resultFileDir;
        this.processName = processName;
        this.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public ChangeStatusProcess(Auth auth, Configuration configuration, String bucket, int status, String resultFileDir,
                               String processName) throws IOException {
        this(auth, configuration, bucket, status, resultFileDir, processName, 0);
    }

    public ChangeStatusProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        ChangeStatusProcess changeStatusProcess = (ChangeStatusProcess)super.clone();
        changeStatusProcess.changeStatus = changeStatus.clone();
        changeStatusProcess.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        try {
            changeStatusProcess.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            e.printStackTrace();
            throw new CloneNotSupportedException();
        }
        return changeStatusProcess;
    }

    public String getProcessName() {
        return this.processName;
    }

    public QiniuException qiniuException() {
        return qiniuException;
    }

    public void processFile(String fileKey, int retryCount) {

        try {
            String result = changeStatus.run(bucket, fileKey, status, retryCount);
            if (!StringUtils.isNullOrEmpty(result)) fileReaderAndWriterMap.writeSuccess(result);
        } catch (QiniuException e) {
            fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + fileKey + "\t" + status + "\t" + e.error());
            if (!e.response.needRetry()) qiniuException = e;
            else e.response.close();
        }
    }

    public void processFile(List<String> keyList, int retryCount) {

        if (keyList == null || keyList.size() == 0) return;
        int times = keyList.size()/1000 + 1;
        for (int i = 0; i < times; i++) {
            List<String> processList = keyList.subList(1000 * i, i == times - 1 ? keyList.size() : 1000 * (i + 1));
            if (processList.size() > 0) {
                try {
                    String result = changeStatus.batchRun(bucket, processList, status, retryCount);
                    if (!StringUtils.isNullOrEmpty(result)) fileReaderAndWriterMap.writeSuccess(result);
                } catch (QiniuException e) {
                    fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + processList + "\t" + status + "\t"
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