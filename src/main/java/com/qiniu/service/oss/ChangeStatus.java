package com.qiniu.service.oss;

import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.util.HttpResponseUtils;

public class ChangeStatus extends OperationBase implements Cloneable {

    public ChangeStatus(QiniuAuth auth, Configuration configuration) {
        super(auth, configuration);
    }

    public ChangeStatus clone() throws CloneNotSupportedException {
        return (ChangeStatus)super.clone();
    }

    public String run(String bucket, String key, short status, int retryCount) throws QiniuException {

        Response response = changeStatusWithRetry(bucket, key, status, retryCount);
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        String reqId = response.reqId;
        response.close();

        return statusCode + "\t" + reqId + "\t" + responseBody;
    }

    synchronized public String batchRun(String bucket, String key, short status, int retryCount) throws QiniuException {
        Response response = batchChangeStatusWithRetry(bucket, key, status, retryCount);
        if (response == null) return null;
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        String reqId = response.reqId;
        response.close();
        batchOperations.clearOps();

        return statusCode + "\t" + reqId + "\t" + responseBody;
    }

    public Response changeStatusWithRetry(String bucket, String key, short status, int retryCount) throws QiniuException {

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

    synchronized public Response batchChangeStatusWithRetry(String bucket, String key, short status, int retryCount) throws QiniuException {

        Response response = null;
        if (batchOperations.getOps().size() < 1000) batchOperations.addChangeStatusOps(bucket, status, key);
        else response = batchWithRetry(batchOperations, retryCount);
        return response;
    }
}