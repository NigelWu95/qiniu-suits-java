package com.qiniu.service.oss;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.service.interfaces.IOssFileProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class UpdateLifecycle extends OperationBase implements IOssFileProcess, Cloneable {

    private String bucket;
    private int days;
    private String resultFileDir;
    private String processName;
    private FileReaderAndWriterMap fileReaderAndWriterMap = new FileReaderAndWriterMap();

    public UpdateLifecycle(Auth auth, Configuration configuration, String bucket, int days, String resultFileDir,
                           String processName, int resultFileIndex) throws IOException {
        super(auth, configuration);
        this.bucket = bucket;
        this.days = days;
        this.resultFileDir = resultFileDir;
        this.processName = processName;
        this.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public UpdateLifecycle(Auth auth, Configuration configuration, String bucket, int days, String resultFileDir,
                           String processName) throws IOException {
        this(auth, configuration, bucket, days, resultFileDir, processName, 0);
    }

    public UpdateLifecycle getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        UpdateLifecycle updateLifecycle = (UpdateLifecycle)super.clone();
        updateLifecycle.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        try {
            updateLifecycle.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            e.printStackTrace();
            throw new CloneNotSupportedException();
        }
        return updateLifecycle;
    }

    public String getProcessName() {
        return this.processName;
    }

    public String run(String bucket, int days, String key, int retryCount) throws QiniuException {

        Response response = updateLifecycleWithRetry(bucket, days, key, retryCount);
        if (response == null) return null;
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        String reqId = response.reqId;
        response.close();

        return statusCode + "\t" + reqId + "\t" + responseBody;
    }

    public Response updateLifecycleWithRetry(String bucket, int days, String key, int retryCount) throws QiniuException {

        Response response = null;
        try {
            response = bucketManager.deleteAfterDays(bucket, key, days);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    response = bucketManager.deleteAfterDays(bucket, key, days);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }

    synchronized public String batchRun(String bucket, int days, List<String> keys, int retryCount) throws QiniuException {

        batchOperations.addDeleteAfterDaysOps(bucket, days, keys.toArray(new String[]{}));
        Response response = batchWithRetry(retryCount);
        if (response == null) return null;
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        String reqId = response.reqId;
        batchOperations.clearOps();
        return statusCode + "\t" + reqId + "\t" + responseBody;
    }

    public void processFile(List<FileInfo> fileInfoList, boolean batch, int retryCount) throws QiniuException {

        if (fileInfoList == null || fileInfoList.size() == 0) return;
        List<String> keyList = fileInfoList.stream().map(fileInfo -> fileInfo.key).collect(Collectors.toList());

        if (batch) {
            List<String> resultList = new ArrayList<>();
            for (String key : keyList) {
                try {
                    String result = run(bucket, days, key, retryCount);
                    if (!StringUtils.isNullOrEmpty(result)) resultList.add(result);
                } catch (QiniuException e) {
                    System.out.println("type failed. " + e.error());
                    fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + days + "\t" + key + "\t" + e.error());
                    if (!e.response.needRetry()) throw e;
                    else e.response.close();
                }
            }
            if (resultList.size() > 0) fileReaderAndWriterMap.writeSuccess(String.join("\n", resultList));
            return;
        }

        int times = fileInfoList.size()/1000 + 1;
        for (int i = 0; i < times; i++) {
            List<String> processList = keyList.subList(1000 * i, i == times - 1 ? keyList.size() : 1000 * (i + 1));
            if (processList.size() > 0) {
                try {
                    String result = batchRun(bucket, days, processList, retryCount);
                    if (!StringUtils.isNullOrEmpty(result)) fileReaderAndWriterMap.writeSuccess(result);
                } catch (QiniuException e) {
                    System.out.println("batch lifecycle failed.  " + e.error());
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
