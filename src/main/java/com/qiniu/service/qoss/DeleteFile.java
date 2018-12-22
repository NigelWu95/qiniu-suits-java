package com.qiniu.service.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager.BatchOperations;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DeleteFile extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    public DeleteFile(Auth auth, Configuration configuration, String bucket, String resultPath,
                      int resultIndex) throws IOException {
        super(auth, configuration, bucket, "delete", resultPath, resultIndex);
    }

    public DeleteFile(Auth auth, Configuration configuration, String bucket, String resultFileDir) throws IOException {
        this(auth, configuration, bucket, resultFileDir, 0);
    }

    protected Response getResponse(Map<String, String> fileInfo) throws QiniuException {
        return bucketManager.delete(bucket, fileInfo.get("key"));
    }

    synchronized protected BatchOperations getOperations(List<Map<String, String>> fileInfoList) {

        List<String> keyList = fileInfoList.stream().map(fileInfo -> fileInfo.get("key")).collect(Collectors.toList());
        batchOperations.addDeleteOp(bucket, keyList.toArray(new String[]{}));
        return batchOperations;
    }
}
