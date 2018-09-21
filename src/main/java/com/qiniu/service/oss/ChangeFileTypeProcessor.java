package com.qiniu.service.oss;

import com.qiniu.common.QiniuBucketManager;
import com.qiniu.common.QiniuBucketManager.*;
import com.qiniu.common.QiniuException;
import com.qiniu.common.QiniuSuitsException;
import com.qiniu.http.Response;

public class ChangeFileTypeProcessor {

    private QiniuBucketManager bucketManager;

    private static volatile ChangeFileTypeProcessor changeFileTypeProcessor = null;

    public ChangeFileTypeProcessor(QiniuBucketManager bucketManager) {
        this.bucketManager = bucketManager;
    }

    public static ChangeFileTypeProcessor getChangeFileTypeProcessor(QiniuBucketManager bucketManager) throws QiniuSuitsException {
        if (changeFileTypeProcessor == null) {
            synchronized (ChangeFileTypeProcessor.class) {
                if (changeFileTypeProcessor == null) {
                    changeFileTypeProcessor = new ChangeFileTypeProcessor(bucketManager);
                }
            }
        }
        return changeFileTypeProcessor;
    }

    public String doFileTypeChange(String bucket, String key, StorageType type) throws QiniuSuitsException {
        Response response = null;
        String respBody = "";

        try {
            response = bucketManager.changeType(bucket, key, type);
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

    public void closeBucketManager() {
        if (bucketManager != null)
            bucketManager.closeResponse();
    }
}