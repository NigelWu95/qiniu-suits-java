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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class UpdateLifecycleProcess implements IOssFileProcess, Cloneable {

    private UpdateLifecycle updateLifecycle;
    private String bucket;
    private int days;
    private String resultFileDir;
    private String processName;
    private FileReaderAndWriterMap fileReaderAndWriterMap = new FileReaderAndWriterMap();

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

    public void processFile(List<FileInfo> fileInfoList, boolean batch, int retryCount) throws QiniuException {

        if (fileInfoList == null || fileInfoList.size() == 0) return;
        List<String> keyList = fileInfoList.stream().map(fileInfo -> fileInfo.key).collect(Collectors.toList());

        if (batch) {
            List<String> resultList = new ArrayList<>();
            for (String key : keyList) {
                String result = updateLifecycle.run(bucket, days, key, retryCount);
                if (!StringUtils.isNullOrEmpty(result)) resultList.add(result);
            }
            if (resultList.size() > 0) fileReaderAndWriterMap.writeSuccess(String.join("\n", resultList));
            return;
        }

        int times = fileInfoList.size()/1000 + 1;
        for (int i = 0; i < times; i++) {
            List<String> processList = keyList.subList(1000 * i, i == times - 1 ? keyList.size() : 1000 * (i + 1));
            if (processList.size() > 0) {
                try {
                    String result = updateLifecycle.batchRun(bucket, days, processList, retryCount);
                    if (!StringUtils.isNullOrEmpty(result)) fileReaderAndWriterMap.writeSuccess(result);
                } catch (QiniuException e) {
                    fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + days + "\t" + processList + "\t"
                            + e.error());
                    if (!e.response.needRetry()) throw e;
                    else e.response.close();
                }
            }
        }
    }

    public void closeResource() {
        fileReaderAndWriterMap.closeWriter();
    }
}
