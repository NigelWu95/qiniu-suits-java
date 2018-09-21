package com.qiniu.service.oss;

import com.qiniu.common.QiniuBucketManager;
import com.qiniu.common.QiniuException;
import com.qiniu.common.QiniuSuitsException;
import com.qiniu.http.Response;
import com.qiniu.util.StringUtils;

public class BucketCopyProcessor {

    private QiniuBucketManager bucketManager;
    private Response response;
    private String srcBucket;
    private String tarBucket;

    private static volatile BucketCopyProcessor bucketCopyProcessor = null;

    public BucketCopyProcessor(QiniuBucketManager bucketManager, String srcBucket, String tarBucket) {
        this.bucketManager = bucketManager;
        this.srcBucket = srcBucket;
        this.tarBucket = tarBucket;
    }

    public static BucketCopyProcessor getBucketCopyProcessor(QiniuBucketManager bucketManager, String srcBucket, String tarBucket) throws QiniuSuitsException {
        if (bucketCopyProcessor == null) {
            synchronized (BucketCopyProcessor.class) {
                if (bucketCopyProcessor == null) {
                    bucketCopyProcessor = new BucketCopyProcessor(bucketManager, srcBucket, tarBucket);
                }
            }
        }
        return bucketCopyProcessor;
    }

    private String copy(String fromBucket, String srcKey, String toBucket, String tarKey) throws QiniuSuitsException {
        String respBody = "";

        try {
            response = bucketManager.copy(fromBucket, srcKey, toBucket, tarKey, false);
            respBody = response.bodyString();
        } catch (QiniuException e) {
            QiniuSuitsException qiniuSuitsException = new QiniuSuitsException("bucket copy error");
            qiniuSuitsException.addToFieldMap("code", String.valueOf(e.code()));
            qiniuSuitsException.addToFieldMap("error", String.valueOf(e.error()));
            qiniuSuitsException.setStackTrace(e.getStackTrace());
            throw qiniuSuitsException;
        }

        return response.statusCode + "\t" + response.reqId + "\t" + respBody;
    }

    public String doBucketCopy(String sourceBucket, String srcKey, String targetBucket, String tarKey) throws QiniuSuitsException {
        return copy(sourceBucket, srcKey, targetBucket, tarKey);
    }

    public String doDefaultBucketCopy(String srcKey, String tarKey) throws QiniuSuitsException {
        return copy(srcBucket, srcKey, tarBucket, tarKey);
    }

    public String doDefaultTargetBucketCopy(String sourceBucket, String srcKey, String tarKey) throws QiniuSuitsException {
        return copy(sourceBucket, srcKey, tarBucket, tarKey);
    }

    public String doDefaultSourceBucketCopy(String targetBucket, String srcKey, String tarKey) throws QiniuSuitsException {
        return copy(srcBucket, srcKey, targetBucket, tarKey);
    }

    public void closeClient() {
        if (response != null) {
            response.close();
        }
    }
}