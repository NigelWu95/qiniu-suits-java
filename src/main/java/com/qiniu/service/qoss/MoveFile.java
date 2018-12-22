package com.qiniu.service.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.persistence.FileMap;
import com.qiniu.sdk.BucketManager.BatchOperations;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MoveFile extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    private String toBucket;
    private String keyPrefix;

    public MoveFile(Auth auth, Configuration configuration, String bucket, String toBucket, String resultPath,
                    int resultIndex) throws IOException {
        super(auth, configuration, bucket, toBucket == null || "".equals(toBucket) ? "rename" : "move",
                resultPath, resultIndex);
        this.toBucket = toBucket;
    }

    public MoveFile(Auth auth, Configuration configuration, String bucket, String toBucket, String resultPath)
            throws IOException {
        this(auth, configuration, bucket, toBucket, resultPath, 0);
    }

    public void setOptions(String keyPrefix) {
        this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
    }

    protected Response getResponse(Map<String, String> fileInfo) throws QiniuException {
        if (toBucket == null || "".equals(toBucket)) {
            return bucketManager.move(bucket, fileInfo.get("key"), toBucket, keyPrefix + fileInfo.get("key"),
                    false);
        } else {
            return bucketManager.rename(bucket, fileInfo.get("key"), keyPrefix + fileInfo.get("newKey"),
                    false);
        }
    }

    synchronized protected BatchOperations getOperations(List<Map<String, String>> fileInfoList) {

        if (toBucket == null || "".equals(toBucket)) {
            fileInfoList.forEach(fileInfo -> batchOperations.addMoveOp(bucket, fileInfo.get("key"), toBucket,
                    keyPrefix + fileInfo.get("key")));
        } else {
            fileInfoList.forEach(fileInfo -> batchOperations.addRenameOp(bucket, fileInfo.get("key"),
                            keyPrefix + fileInfo.get("newKey")));
        }

        return batchOperations;
    }
}
