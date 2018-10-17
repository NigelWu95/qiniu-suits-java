package com.qiniu.service.oss;

import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuBucketManager;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.util.HttpResponseUtils;

import java.util.List;

public class BucketCopy extends OperationBase implements Cloneable {

    public BucketCopy(QiniuAuth auth, Configuration configuration) {
        super(auth, configuration);
    }

    public BucketCopy clone() throws CloneNotSupportedException {
        return (BucketCopy)super.clone();
    }

    public String run(String fromBucket, String srcKey, String toBucket, String tarKey, String keyPrefix, boolean force, int retryCount) throws QiniuException {

        Response response = copyWithRetry(fromBucket, srcKey, toBucket, tarKey, keyPrefix, force, retryCount);
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        String reqId = response.reqId;
        response.close();

        return statusCode + "\t" + reqId + "\t" + responseBody;
    }

    public Response copyWithRetry(String fromBucket, String srcKey, String toBucket, String tarKey, String keyPrefix, boolean force, int retryCount) throws QiniuException {

        Response response = null;
        try {
            response = bucketManager.copy(fromBucket, srcKey, toBucket, tarKey, force);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    System.out.println("status " + fromBucket + ":" + srcKey + " to " + toBucket + ":" + tarKey + " " + e1.error() + ", last "
                            + retryCount + " times retry...");
                    response = bucketManager.copy(fromBucket, srcKey, toBucket, tarKey, false);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }

    synchronized public String batchRun(String fromBucket, String toBucket, List<String> keys, String keyPrefix, boolean force, int retryCount) throws QiniuException {

        batchOperations.addCopyOps(fromBucket, toBucket, force, keyPrefix, keys.toArray(new String[]{}));
        Response response = batchWithRetry(retryCount, "batch copy " + fromBucket + ":" + keys + " to " + toBucket + " " + keyPrefix);
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        String reqId = response.reqId;
        batchOperations.clearOps();
        return statusCode + "\t" + reqId + "\t" + responseBody;
    }
}