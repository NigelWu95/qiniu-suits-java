package com.qiniu.service.oss;

import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuBucketManager;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.util.HttpResponseUtils;

public class BucketCopy {

    private QiniuBucketManager bucketManager;
    private String srcBucket;
    private String tarBucket;

    private static volatile BucketCopy bucketCopy = null;

    public BucketCopy(QiniuAuth auth, Configuration configuration, String srcBucket, String tarBucket) {
        this.bucketManager = new QiniuBucketManager(auth, configuration);
        this.srcBucket = srcBucket;
        this.tarBucket = tarBucket;
    }

    public static BucketCopy getInstance(QiniuAuth auth, Configuration configuration, String srcBucket, String tarBucket) {
        if (bucketCopy == null) {
            synchronized (BucketCopy.class) {
                if (bucketCopy == null) {
                    bucketCopy = new BucketCopy(auth, configuration, srcBucket, tarBucket);
                }
            }
        }
        return bucketCopy;
    }

    private String copy(String fromBucket, String srcKey, String toBucket, String tarKey, boolean force, int retryCount) throws QiniuException {
        Response response = copyWithRetry(fromBucket, srcKey, toBucket, tarKey, force, retryCount);
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        String reqId = response.reqId;
        response.close();

        return statusCode + "\t" + reqId + "\t" + responseBody;
    }

    public String run(String sourceBucket, String srcKey, String targetBucket, String tarKey, boolean force, int retryCount) throws QiniuException {
        return copy(sourceBucket, srcKey, targetBucket, tarKey, force, retryCount);
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
                    System.out.println(e1.getMessage() + ", last " + retryCount + " times retry...");
                    response = bucketManager.copy(fromBucket, srcKey, toBucket, tarKey, false);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }

    public void closeBucketManager() {
        if (bucketManager != null)
            bucketManager.closeResponse();
    }
}