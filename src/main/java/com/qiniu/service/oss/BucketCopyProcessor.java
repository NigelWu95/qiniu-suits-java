package com.qiniu.service.oss;

import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuBucketManager;
import com.qiniu.common.QiniuException;
import com.qiniu.common.QiniuSuitsException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.StringMap;
import com.qiniu.util.UrlSafeBase64;

public class BucketCopyProcessor {

    private QiniuBucketManager bucketManager;
    private String srcBucket;
    private String tarBucket;

    private static volatile BucketCopyProcessor bucketCopyProcessor = null;

    public BucketCopyProcessor(QiniuAuth auth, Configuration configuration, String srcBucket, String tarBucket) {
        this.bucketManager = new QiniuBucketManager(auth, configuration);
        this.srcBucket = srcBucket;
        this.tarBucket = tarBucket;
    }

    public static BucketCopyProcessor getBucketCopyProcessor(QiniuAuth auth, Configuration configuration,
                                                             String srcBucket, String tarBucket) {
        if (bucketCopyProcessor == null) {
            synchronized (BucketCopyProcessor.class) {
                if (bucketCopyProcessor == null) {
                    bucketCopyProcessor = new BucketCopyProcessor(auth, configuration, srcBucket, tarBucket);
                }
            }
        }
        return bucketCopyProcessor;
    }

    private String copy(String fromBucket, String srcKey, String toBucket, String tarKey, boolean force, int retryCount) throws QiniuSuitsException {
        Response response = null;
        String respBody = "";

        try {
            response = changeStatusWithRetry(fromBucket, srcKey, toBucket, tarKey, force, retryCount);
            respBody = response.bodyString();
        } catch (QiniuException e) {
            QiniuSuitsException qiniuSuitsException = new QiniuSuitsException("bucket copy error");
            qiniuSuitsException.addToFieldMap("code", String.valueOf(e.code()));
            qiniuSuitsException.addToFieldMap("error", String.valueOf(e.error()));
            qiniuSuitsException.setStackTrace(e.getStackTrace());
            throw qiniuSuitsException;
        } finally {
            if (response != null)
                response.close();
        }

        return response.statusCode + "\t" + response.reqId + "\t" + respBody;
    }

    public String doBucketCopy(String sourceBucket, String srcKey, String targetBucket, String tarKey, boolean force, int retryCount) throws QiniuSuitsException {
        return copy(sourceBucket, srcKey, targetBucket, tarKey, false, retryCount);
    }

    public String doDefaultBucketCopy(String srcKey, String tarKey, boolean force, int retryCount) throws QiniuSuitsException {
        return copy(srcBucket, srcKey, tarBucket, tarKey, false, retryCount);
    }

    public String doDefaultTargetBucketCopy(String sourceBucket, String srcKey, String tarKey, boolean force, int retryCount) throws QiniuSuitsException {
        return copy(sourceBucket, srcKey, tarBucket, tarKey, false, retryCount);
    }

    public String doDefaultSourceBucketCopy(String targetBucket, String srcKey, String tarKey, boolean force, int retryCount) throws QiniuSuitsException {
        return copy(srcBucket, srcKey, targetBucket, tarKey, false, retryCount);
    }

    private Response changeStatusWithRetry(String fromBucket, String srcKey, String toBucket, String tarKey, boolean force, int retryCount) throws QiniuSuitsException {

        Response response = null;

        try {
            response = bucketManager.copy(fromBucket, srcKey, toBucket, tarKey, false);
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