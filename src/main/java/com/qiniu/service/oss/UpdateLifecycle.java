package com.qiniu.service.oss;

import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuBucketManager;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.util.HttpResponseUtils;

import java.util.List;

public class UpdateLifecycle extends OperationBase implements Cloneable {

    public UpdateLifecycle(QiniuAuth auth, Configuration configuration) {
        super(auth, configuration);
    }

    public UpdateLifecycle clone() throws CloneNotSupportedException {
        return (UpdateLifecycle) super.clone();
    }

    public String run(String bucket, String key, int days, int retryCount) throws QiniuException {

        Response response = updateLifecycleWithRetry(bucket, key, days, retryCount);
        if (response == null) return null;
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        String reqId = response.reqId;
        response.close();

        return statusCode + "\t" + reqId + "\t" + responseBody;
    }

    public Response updateLifecycleWithRetry(String bucket, String key, int days, int retryCount) throws QiniuException {

        Response response = null;
        try {
            response = bucketManager.deleteAfterDays(bucket, key, days);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    System.out.println("lifecycle " + bucket + ":" + key + " to " + days + " " + e1.error() + ", last "
                            + retryCount + " times retry...");
                    response = bucketManager.deleteAfterDays(bucket, key, days);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }

    synchronized public String batchRun(String bucket, List<String> keys, int days, int retryCount) throws QiniuException {

        batchOperations.addDeleteAfterDaysOps(bucket, days, keys.toArray(new String[]{}));
        Response response = batchWithRetry(retryCount, "batch lifecycle " + bucket + ":" + keys + " to " + days);
        if (response == null) return null;
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        String reqId = response.reqId;
        batchOperations.clearOps();
        return statusCode + "\t" + reqId + "\t" + responseBody;
    }
}