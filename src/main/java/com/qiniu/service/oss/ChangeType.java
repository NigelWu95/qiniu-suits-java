package com.qiniu.service.oss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.StorageType;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;

import java.util.List;

public class ChangeType extends OperationBase implements Cloneable {

    public ChangeType(Auth auth, Configuration configuration) {
        super(auth, configuration);
    }

    public ChangeType clone() throws CloneNotSupportedException {
        return (ChangeType) super.clone();
    }

    public String run(String bucket, String key, int type, int retryCount) throws QiniuException {

        Response response = changeTypeWithRetry(bucket, key, type, retryCount);
        if (response == null) return null;
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        String reqId = response.reqId;
        response.close();

        return statusCode + "\t" + reqId + "\t" + responseBody;
    }

    public Response changeTypeWithRetry(String bucket, String key, int type, int retryCount) throws QiniuException {

        Response response = null;
        StorageType storageType = type == 0 ? StorageType.COMMON : StorageType.INFREQUENCY;
        try {
            response = bucketManager.changeType(bucket, key, storageType);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    System.out.println("type " + bucket + ":" + key + " to " + type + " " + e1.error() + ", last "
                            + retryCount + " times retry...");
                    response = bucketManager.changeType(bucket, key, storageType);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }

    synchronized public String batchRun(String bucket, List<String> keys, int type, int retryCount) throws QiniuException {

        batchOperations.addChangeTypeOps(bucket, type == 0 ? StorageType.COMMON : StorageType.INFREQUENCY, keys.toArray(new String[]{}));
        Response response = batchWithRetry(retryCount, "batch type " + bucket + ":" + keys + " to " + type);
        if (response == null) return null;
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        String reqId = response.reqId;
        batchOperations.clearOps();
        return statusCode + "\t" + reqId + "\t" + responseBody;
    }
}