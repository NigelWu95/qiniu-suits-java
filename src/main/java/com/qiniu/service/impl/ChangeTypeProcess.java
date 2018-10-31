package com.qiniu.service.impl;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.service.oss.ChangeType;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.List;

public class ChangeTypeProcess implements IOssFileProcess {

    private ChangeType changeType;
    private String bucket;
    private int fileType;
    private String resultFileDir;
    private FileReaderAndWriterMap fileReaderAndWriterMap = new FileReaderAndWriterMap();
    private QiniuException qiniuException = null;

    public ChangeTypeProcess(Auth auth, Configuration configuration, String bucket, int fileType, String resultFileDir,
            int resultFileIndex) throws IOException {
        this.changeType = new ChangeType(auth, configuration);
        this.bucket = bucket;
        this.fileType = fileType;
        this.resultFileDir = resultFileDir;
        this.fileReaderAndWriterMap.initWriter(resultFileDir, "type", resultFileIndex);
    }

    public ChangeTypeProcess(Auth auth, Configuration configuration, String bucket, int fileStatus, String resultFileDir)
            throws IOException {
        this(auth, configuration, bucket, fileStatus, resultFileDir, 0);
    }

    public ChangeTypeProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        ChangeTypeProcess changeTypeProcess = (ChangeTypeProcess)super.clone();
        changeTypeProcess.changeType = changeType.clone();
        changeTypeProcess.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        try {
            changeTypeProcess.fileReaderAndWriterMap.initWriter(resultFileDir, "type", resultFileIndex);
        } catch (IOException e) {
            e.printStackTrace();
            throw new CloneNotSupportedException();
        }
        return changeTypeProcess;
    }

    public QiniuException qiniuException() {
        return qiniuException;
    }

    public void processFile(String fileKey, int retryCount) {

        try {
            String result = changeType.run(bucket, fileKey, fileType, retryCount);
            if (!StringUtils.isNullOrEmpty(result)) fileReaderAndWriterMap.writeSuccess(result);
        } catch (QiniuException e) {
            fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + fileKey + "\t" + fileType + "\t" + e.error());
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
                    String result = changeType.batchRun(bucket, processList, fileType, retryCount);
                    if (!StringUtils.isNullOrEmpty(result)) fileReaderAndWriterMap.writeSuccess(result);
                } catch (QiniuException e) {
                    fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + processList + "\t" + fileType + "\t" + e.error());
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