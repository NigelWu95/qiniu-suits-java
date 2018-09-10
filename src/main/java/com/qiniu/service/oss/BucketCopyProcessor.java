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

    public String doBucketCopy(String sourceBucket, String srcKey, String tarKey) throws QiniuSuitsException {
        String respBody = "";

        try {
            response = bucketManager.copy(StringUtils.isNullOrEmpty(sourceBucket) ? srcBucket : sourceBucket, srcKey, tarBucket, tarKey, false);
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

    public void closeClient() {
        if (response != null) {
            response.close();
        }
    }
}