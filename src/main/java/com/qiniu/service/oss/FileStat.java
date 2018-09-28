package com.qiniu.service.oss;

import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuBucketManager;
import com.qiniu.common.QiniuException;
import com.qiniu.storage.Configuration;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.JSONConvertUtils;

public class FileStat {

    private QiniuBucketManager bucketManager;

    private static volatile FileStat fileStat = null;

    public FileStat(QiniuAuth auth, Configuration configuration) {
        this.bucketManager = new QiniuBucketManager(auth, configuration);
    }

    public FileStat(QiniuBucketManager bucketManager) {
        this.bucketManager = bucketManager;
    }

    public static FileStat getInstance(QiniuAuth auth, Configuration configuration) {
        if (fileStat == null) {
            synchronized (ChangeType.class) {
                if (fileStat == null) {
                    fileStat = new FileStat(auth, configuration);
                }
            }
        }
        return fileStat;
    }

    public String run(String bucket, String fileKey, int retryCount) throws QiniuException {

        String fileInfoStr = "";

        try {
            fileInfoStr = JSONConvertUtils.toJson(bucketManager.stat(bucket, fileKey));
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    System.out.println("stat" + e1.error() + ", last " + retryCount + " times retry...");
                    fileInfoStr = JSONConvertUtils.toJson(bucketManager.stat(bucket, fileKey));
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return fileInfoStr;
    }

    public void closeBucketManager() {
        if (bucketManager != null)
            bucketManager.closeResponse();
    }
}