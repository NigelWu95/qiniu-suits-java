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

public class ChangeStatus extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    private int status;

    public ChangeStatus(Auth auth, Configuration configuration, String bucket, int status, String resultPath,
                        int resultIndex) throws IOException {
        super(auth, configuration, bucket, "status", resultPath, resultIndex);
        this.status = status;
    }

    public ChangeStatus(Auth auth, Configuration configuration, String bucket, int status, String resultPath)
            throws IOException {
        this(auth, configuration, bucket, status, resultPath, 0);
    }

    public ChangeStatus getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        ChangeStatus changeStatus = (ChangeStatus)super.clone();
        try {
            changeStatus.fileMap.initWriter(resultPath, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return changeStatus;
    }

    protected Response getResponse(Map<String, String> fileInfo) throws QiniuException {
        return bucketManager.changeStatus(bucket, fileInfo.get("key"), status);
    }

    synchronized protected BatchOperations getOperations(List<Map<String, String>> fileInfoList){
        List<String> keyList = fileInfoList.stream().map(fileInfo -> fileInfo.get("key")).collect(Collectors.toList());
        return batchOperations.addChangeStatusOps(bucket, status, keyList.toArray(new String[]{}));
    }
}
