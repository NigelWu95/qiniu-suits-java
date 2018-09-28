package com.qiniu.service.oss;

import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuBucketManager;
import com.qiniu.common.QiniuBucketManager.*;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.util.HttpResponseUtils;

public class ChangeType {

    private QiniuBucketManager bucketManager;

    private static volatile ChangeType changeType = null;

    public ChangeType(QiniuAuth auth, Configuration configuration) {
        this.bucketManager = new QiniuBucketManager(auth, configuration);
    }

    public static ChangeType getInstance(QiniuAuth auth, Configuration configuration) {
        if (changeType == null) {
            synchronized (ChangeType.class) {
                if (changeType == null) {
                    changeType = new ChangeType(auth, configuration);
                }
            }
        }
        return changeType;
    }

    public String run(String bucket, String key, short type, int retryCount) throws QiniuException {
        Response response = changeTypeWithRetry(bucket, key, type, retryCount);
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        String reqId = response.reqId;
        response.close();

        return statusCode + "\t" + reqId + "\t" + responseBody;
    }

    public Response changeTypeWithRetry(String bucket, String key, short type, int retryCount) throws QiniuException {
        Response response = null;
        StorageType storageType = type == 0 ? StorageType.COMMON : StorageType.INFREQUENCY;

        try {
            response = bucketManager.changeType(bucket, key, storageType);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    System.out.println("type " + e1.error() + ", last " + retryCount + " times retry...");
                    response = bucketManager.changeType(bucket, key, storageType);
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