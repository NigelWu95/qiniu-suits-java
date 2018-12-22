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

public class CopyFile extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    private String toBucket;
    private boolean keepKey;
    private String keyPrefix;

    public CopyFile(Auth auth, Configuration configuration, String bucket, String toBucket, String resultPath,
                    int resultIndex) throws IOException {
        super(auth, configuration, bucket, "copy", resultPath, resultIndex);
        this.toBucket = toBucket;
    }

    public CopyFile(Auth auth, Configuration configuration, String bucket, String toBucket, String resultPath)
            throws IOException {
        this(auth, configuration, bucket, toBucket, resultPath, 0);
    }

    public void setOptions(boolean keepKey, String keyPrefix) {
        this.keepKey = keepKey;
        this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
    }

    protected Response getResponse(Map<String, String> fileInfo) throws QiniuException {
        return bucketManager.copy(bucket, fileInfo.get("key"), toBucket, keepKey ? keyPrefix +
                fileInfo.get("key") : null, false);
    }

    synchronized protected BatchOperations getOperations(List<Map<String, String>> fileInfoList) {

        List<String> keyList = fileInfoList.stream().map(fileInfo -> fileInfo.get("key")).collect(Collectors.toList());
        if (keepKey) {
            keyList.forEach(fileKey -> batchOperations.addCopyOp(bucket, fileKey, toBucket, keyPrefix + fileKey));
        } else {
            keyList.forEach(fileKey -> batchOperations.addCopyOp(bucket, fileKey, toBucket, null));
        }

        return batchOperations;
    }
}
