package com.qiniu.service.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager.*;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FileStat extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    private void initBaseParams() {
        this.processName = "stat";
    }

    public FileStat(Auth auth, Configuration configuration, String bucket, String resultFileDir) {
        super(auth, configuration, bucket, resultFileDir);
        initBaseParams();
    }

    public FileStat(Auth auth, Configuration configuration, String bucket, String resultFileDir, int resultFileIndex)
            throws IOException {
        this(auth, configuration, bucket, resultFileDir);
        this.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public FileStat getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        FileStat fileStat = (FileStat)super.clone();
        try {
            fileStat.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return fileStat;
    }

    public void setBatch(boolean batch) {}

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public String getProcessName() {
        return this.processName;
    }

    protected Response getResponse(Map<String, String> fileInfo) {
        return null;
    }

    public String statWithRetry(String key, int retryCount) throws QiniuException {

        String stat = "";
        try {
            stat = bucketManager.statResponse(bucket, key).bodyString();
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    stat = bucketManager.statResponse(bucket, key).bodyString();
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return stat;
    }

    synchronized protected BatchOperations getOperations(List<Map<String, String>> fileInfoList) {
        return null;
    }

    public void processLine(List<Map<String, String>> fileInfoList) throws QiniuException {

        List<String> resultList = new ArrayList<>();
        for (Map<String, String> fileInfo : fileInfoList) {
            try {
                String stat = statWithRetry(fileInfo.get("key"), retryCount);
                if (stat != null) resultList.add(fileInfo.get("key") + "\t" + stat);
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileMap, fileInfo.get("key"));
            }
        }
        if (resultList.size() > 0) fileMap.writeSuccess(String.join("\n", resultList));
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}
