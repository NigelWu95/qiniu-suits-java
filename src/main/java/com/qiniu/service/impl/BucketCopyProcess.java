package com.qiniu.service.impl;

import com.google.gson.JsonObject;
import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.service.oss.BucketCopy;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.JsonConvertUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;

public class BucketCopyProcess implements IOssFileProcess, Cloneable {

    private BucketCopy bucketCopy;
    private String resultFileDir;
    private FileReaderAndWriterMap fileReaderAndWriterMap = new FileReaderAndWriterMap();
    private String srcBucket;
    private String tarBucket;
    private String keyPrefix;
    private QiniuException qiniuException = null;

    public BucketCopyProcess(QiniuAuth auth, Configuration configuration, String sourceBucket, String targetBucket,
                             String keyPrefix, String resultFileDir) throws IOException {
        this.bucketCopy = new BucketCopy(auth, configuration);
        this.bucketCopy.setBucket(sourceBucket, targetBucket);
        this.resultFileDir = resultFileDir;
        this.fileReaderAndWriterMap.initWriter(resultFileDir, "copy");
        this.srcBucket = sourceBucket;
        this.tarBucket = targetBucket;
        this.keyPrefix = StringUtils.isNullOrEmpty(keyPrefix) ? "" : keyPrefix;
    }

    public BucketCopyProcess clone() throws CloneNotSupportedException {
        BucketCopyProcess bucketCopyProcess = (BucketCopyProcess)super.clone();
        bucketCopyProcess.bucketCopy = bucketCopy.clone();
        bucketCopyProcess.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        try {
            bucketCopyProcess.fileReaderAndWriterMap.initWriter(resultFileDir, "copy");
        } catch (IOException e) {
            e.printStackTrace();
            throw new CloneNotSupportedException();
        }
        return bucketCopyProcess;
    }

    public QiniuException qiniuException() {
        return qiniuException;
    }

    private void bucketChangeTypeResult(String sourceBucket, String srcKey, String targetBucket, String tarKey, boolean force,
                                       int retryCount, boolean batch) {
        try {
            String result = batch ?
                    bucketCopy.batchRun(sourceBucket, srcKey, targetBucket, keyPrefix + tarKey, force, retryCount) :
                    bucketCopy.run(sourceBucket, srcKey, targetBucket, keyPrefix + tarKey, force, retryCount);
            if (!StringUtils.isNullOrEmpty(result)) fileReaderAndWriterMap.writeSuccess(result);
        } catch (QiniuException e) {
            if (!e.response.needRetry()) qiniuException = e;
            if (batch) fileReaderAndWriterMap.writeErrorOrNull(bucketCopy.getBatchOps() + "\t" + e.error());
            else fileReaderAndWriterMap.writeErrorOrNull(sourceBucket + "\t" + srcKey + "\t" + targetBucket + "\t" +
                    tarKey + "\t" + e.error());
            e.response.close();
        }
    }

    public void processFile(FileInfo fileInfo, int retryCount, boolean batch) {
        String key = fileInfo.key;
        bucketChangeTypeResult(srcBucket, key, tarBucket, key, false, retryCount, batch);
    }

    public void checkBatchProcess(int retryCount) {
        try {
            String result = bucketCopy.batchCheckRun(retryCount);
            if (!StringUtils.isNullOrEmpty(result)) fileReaderAndWriterMap.writeSuccess(result);
        } catch (QiniuException e) {
            if (!e.response.needRetry()) qiniuException = e;
            fileReaderAndWriterMap.writeErrorOrNull(bucketCopy.getBatchOps() + "\t" + e.error());
            e.response.close();
        }
    }

    public void closeResource() {
        fileReaderAndWriterMap.closeWriter();
        if (bucketCopy != null)
            bucketCopy.closeBucketManager();
    }
}