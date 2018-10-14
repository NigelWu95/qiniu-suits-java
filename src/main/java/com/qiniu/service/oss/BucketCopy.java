package com.qiniu.service.oss;

import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuBucketManager;
import com.qiniu.common.QiniuBucketManager.*;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.util.HttpResponseUtils;

import java.util.ArrayList;

public class BucketCopy implements Cloneable {

    private QiniuAuth auth;
    private Configuration configuration;
    private String srcBucket;
    private String tarBucket;
    private QiniuBucketManager bucketManager;
    private BatchOperations batchOperations;

    public BucketCopy(QiniuAuth auth, Configuration configuration, String srcBucket, String tarBucket) {
        this.auth = auth;
        this.configuration = configuration;
        this.srcBucket = srcBucket;
        this.tarBucket = tarBucket;
        this.bucketManager = new QiniuBucketManager(auth, configuration);
        this.batchOperations = new BatchOperations();
    }

    public BucketCopy clone() throws CloneNotSupportedException {
        BucketCopy bucketCopy = (BucketCopy)super.clone();
        bucketCopy.bucketManager = new QiniuBucketManager(auth, configuration);
        bucketCopy.batchOperations = new BatchOperations();
        return bucketCopy;
    }

    public ArrayList<String> getBatchOps() {
        return batchOperations.getOps();
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

    public String batchRun(String fromBucket, String srcKey, String toBucket, String tarKey, boolean force, int retryCount) throws QiniuException {
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

    public Response batchCopyWithRetry(String fromBucket, String srcKey, String toBucket, String tarKey, boolean force,
                                       int retryCount) throws QiniuException {
        Response response = null;

        try {
            if (batchOperations.getOps().size() < 1000) batchOperations.addCopyOps(fromBucket, srcKey, toBucket, tarKey, force);
            else response = bucketManager.batch(batchOperations);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    System.out.println("copy " + e1.error() + ", last " + retryCount + " times retry...");
                    response = bucketManager.batch(batchOperations);
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