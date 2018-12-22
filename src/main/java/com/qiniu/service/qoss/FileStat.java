package com.qiniu.service.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager.*;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FileStat extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    public FileStat(Auth auth, Configuration configuration, String bucket, String resultPath, int resultIndex)
            throws IOException {
        super(auth, configuration, bucket, "stat", resultPath, resultIndex);
    }

    public FileStat(Auth auth, Configuration configuration, String bucket, String resultPath) throws IOException {
        this(auth, configuration, bucket, resultPath, 0);
    }

    public FileStat getNewInstance(int resultIndex) throws CloneNotSupportedException {
        FileStat fileStat = (FileStat)super.clone();
        try {
            fileStat.fileMap.initWriter(resultPath, processName, resultIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return fileStat;
    }

    protected Response getResponse(Map<String, String> fileInfo) throws QiniuException {
        return bucketManager.statResponse(bucket, fileInfo.get("key"));
    }

    synchronized protected BatchOperations getOperations(List<Map<String, String>> fileInfoList) {
        List<String> keyList = fileInfoList.stream().map(fileInfo -> fileInfo.get("key")).collect(Collectors.toList());
        return batchOperations.addStatOps(bucket, keyList.toArray(new String[]{}));
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}
