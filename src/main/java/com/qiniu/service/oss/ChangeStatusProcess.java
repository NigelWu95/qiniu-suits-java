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

public class ChangeStatusProcess extends OperationBase implements IOssFileProcess, Cloneable {

    private String bucket;
    private int status;
    private String resultFileDir;
    private String processName;
    private FileReaderAndWriterMap fileReaderAndWriterMap = new FileReaderAndWriterMap();

    public ChangeStatusProcess(Auth auth, Configuration configuration, String bucket, int status, String resultFileDir,
                               String processName, int resultFileIndex) throws IOException {
        super(auth, configuration);
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

    public String run(String bucket, int status, String key, int retryCount) throws QiniuException {

        Response response = changeStatusWithRetry(bucket, status, key, retryCount);
        if (response == null) return null;
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        String reqId = response.reqId;
        response.close();

        return statusCode + "\t" + reqId + "\t" + responseBody;
    }

    public Response changeStatusWithRetry(String bucket, int status, String key, int retryCount) throws QiniuException {

        Response response = null;
        try {
            response = bucketManager.changeStatus(bucket, key, status);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    System.out.println("status " + bucket + ":" + key + " to " + status + " " + e1.error() + ", last "
                            + retryCount + " times retry...");
                    response = bucketManager.changeStatus(bucket, key, status);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }

    synchronized public String batchRun(String bucket, int status, List<String> keys, int retryCount)
            throws QiniuException {

        batchOperations.addChangeStatusOps(bucket, status, keys.toArray(new String[]{}));
        Response response = batchWithRetry(retryCount, "batch status " + bucket + ":" + keys + " to " + status);
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
                String result = run(bucket, status, key, retryCount);
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
                    String result = batchRun(bucket, status, processList, retryCount);
                    if (!StringUtils.isNullOrEmpty(result)) fileReaderAndWriterMap.writeSuccess(result);
                } catch (QiniuException e) {
                    fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + status + "\t" + processList + "\t"
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
