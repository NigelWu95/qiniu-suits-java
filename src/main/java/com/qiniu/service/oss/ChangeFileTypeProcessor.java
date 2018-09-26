package com.qiniu.service.oss;

import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuBucketManager;
import com.qiniu.common.QiniuBucketManager.*;
import com.qiniu.common.QiniuException;
import com.qiniu.common.QiniuSuitsException;
import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.JSONConvertUtils;

public class ChangeFileTypeProcessor {

    private QiniuBucketManager bucketManager;

    private static volatile ChangeFileTypeProcessor changeFileTypeProcessor = null;

    public ChangeFileTypeProcessor(QiniuAuth auth, Configuration configuration) {
        this.bucketManager = new QiniuBucketManager(auth, configuration);
    }

    public static ChangeFileTypeProcessor getChangeFileTypeProcessor(QiniuAuth auth, Configuration configuration) {
        if (changeFileTypeProcessor == null) {
            synchronized (ChangeFileTypeProcessor.class) {
                if (changeFileTypeProcessor == null) {
                    changeFileTypeProcessor = new ChangeFileTypeProcessor(auth, configuration);
                }
            }
        }
        return changeFileTypeProcessor;
    }

    public String doFileTypeChange(String bucket, String key, short type) throws QiniuSuitsException {

        return doFileTypeChange(bucket, key, type, 0);
    }

    public String doFileTypeChange(String bucket, String key, short type, int retryCount) throws QiniuSuitsException {
        Response response = null;
        String respBody;

        try {
            response = changeTypeWithRetry(bucket, key, type, retryCount);
            respBody = response.bodyString();
        } catch (QiniuException e) {
            QiniuSuitsException qiniuSuitsException = new QiniuSuitsException("change file type error");
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

    private Response changeTypeWithRetry(String bucket, String key, short type, int retryCount) throws QiniuSuitsException {
        Response response = null;
        StorageType storageType = type == 0 ? StorageType.COMMON : StorageType.INFREQUENCY;

        try {
            response = bucketManager.changeType(bucket, key, storageType);
        } catch (QiniuException e1) {
            if (retryCount <= 0)
                throw new QiniuSuitsException(e1);
            while (retryCount > 0) {
                try {
                    System.out.println(e1.getMessage() + ", last " + retryCount + " times retry...");
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