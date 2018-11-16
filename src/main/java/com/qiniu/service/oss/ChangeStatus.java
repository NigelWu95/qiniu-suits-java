package com.qiniu.service.oss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;

import java.util.List;

public class ChangeStatus extends OperationBase implements Cloneable {

    public ChangeStatus(Auth auth, Configuration configuration) {
        super(auth, configuration);
    }

    public ChangeStatus clone() throws CloneNotSupportedException {
        return (ChangeStatus)super.clone();
    }

    public String run(String bucket, String key, int status, int retryCount) throws QiniuException {

        Response response = changeStatusWithRetry(bucket, key, status, retryCount);
        if (response == null) return null;
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        String reqId = response.reqId;
        response.close();

        return statusCode + "\t" + reqId + "\t" + responseBody;
    }

    public Response changeStatusWithRetry(String bucket, String key, int status, int retryCount) throws QiniuException {

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

    synchronized public String batchRun(String bucket, List<String> keys, int status, int retryCount)
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
}