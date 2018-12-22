package com.qiniu.service.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager.*;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class UpdateLifecycle extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    private int days;

    public UpdateLifecycle(Auth auth, Configuration configuration, String bucket, int days, String resultPath,
                           int resultIndex) throws IOException {
        super(auth, configuration, bucket, "lifecycle", resultPath, resultIndex);
        this.days = days;
    }

    public UpdateLifecycle(Auth auth, Configuration configuration, String bucket, int days, String resultPath)
            throws IOException {
        this(auth, configuration, bucket, days, resultPath, 0);
    }

    protected Response getResponse(Map<String, String> fileInfo) throws QiniuException {
        return bucketManager.deleteAfterDays(bucket, fileInfo.get("key"), days);
    }

    synchronized protected BatchOperations getOperations(List<Map<String, String>> fileInfoList){
        List<String> keyList = fileInfoList.stream().map(fileInfo -> fileInfo.get("key")).collect(Collectors.toList());
        return batchOperations.addDeleteAfterDaysOps(bucket, days, keyList.toArray(new String[]{}));
    }
}
