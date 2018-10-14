package com.qiniu.service.oss;

import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.util.HttpResponseUtils;

public class BucketCopy extends OperationBase implements Cloneable {

    private String srcBucket;
    private String tarBucket;

    public BucketCopy(QiniuAuth auth, Configuration configuration) {
        super(auth, configuration);
    }

    public void setBucket(String srcBucket, String tarBucket) {
        this.srcBucket = srcBucket;
        this.tarBucket = tarBucket;
    }

    public BucketCopy clone() throws CloneNotSupportedException {
        return (BucketCopy)super.clone();
    }

    private String copy(String fromBucket, String srcKey, String toBucket, String tarKey, boolean force, int retryCount) throws QiniuException {
        Response response = copyWithRetry(fromBucket, srcKey, toBucket, tarKey, force, retryCount);
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        String reqId = response.reqId;
        response.close();

        return statusCode + "\t" + reqId + "\t" + responseBody;
    }

    public String run(String fromBucket, String srcKey, String toBucket, String tarKey, boolean force, int retryCount) throws QiniuException {
        return copy(fromBucket, srcKey, toBucket, tarKey, force, retryCount);
    }

    synchronized public String batchRun(String fromBucket, String srcKey, String toBucket, String tarKey, boolean force, int retryCount) throws QiniuException {
        Response response = batchCopyWithRetry(fromBucket, srcKey, toBucket, tarKey, force, retryCount);
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        String reqId = response.reqId;
        response.close();
        batchOperations.clearOps();

        return statusCode + "\t" + reqId + "\t" + responseBody;
    }

    public String runWithDefaultBucket(String srcKey, String tarKey, boolean force, int retryCount) throws QiniuException {
        return copy(srcBucket, srcKey, tarBucket, tarKey, force, retryCount);
    }

    public String runWithDefaultTargetBucket(String sourceBucket, String srcKey, String tarKey, boolean force, int retryCount) throws QiniuException {
        return copy(sourceBucket, srcKey, tarBucket, tarKey, force, retryCount);
    }

    public String runWithDefaultSourceBucket(String targetBucket, String srcKey, String tarKey, boolean force, int retryCount) throws QiniuException {
        return copy(srcBucket, srcKey, targetBucket, tarKey, force, retryCount);
    }

    public Response copyWithRetry(String fromBucket, String srcKey, String toBucket, String tarKey, boolean force, int retryCount) throws QiniuException {

        Response response = null;

        try {
            response = bucketManager.copy(fromBucket, srcKey, toBucket, tarKey, force);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    System.out.println("copy " + e1.error() + ", last " + retryCount + " times retry...");
                    response = bucketManager.copy(fromBucket, srcKey, toBucket, tarKey, false);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }

    synchronized public Response batchCopyWithRetry(String fromBucket, String srcKey, String toBucket, String tarKey, boolean force,
                                       int retryCount) throws QiniuException {
        Response response = null;
        if (batchOperations.getOps().size() < 1000) batchOperations.addCopyOps(fromBucket, srcKey, toBucket, tarKey, force);
        else response = batchWithRetry(batchOperations, retryCount);
        return response;
    }
}