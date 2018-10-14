package com.qiniu.service.impl;

import com.google.gson.JsonObject;
import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.interfaces.IUrlItemProcess;
import com.qiniu.service.auvideo.M3U8Manager;
import com.qiniu.service.auvideo.VideoTS;
import com.qiniu.service.oss.BucketCopy;
import com.qiniu.storage.Configuration;
import com.qiniu.util.DateUtils;
import com.qiniu.util.JSONConvertUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
        this.bucketCopy = new BucketCopy(auth, configuration, sourceBucket, targetBucket);
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
            String bucketCopyResult = batch ?
                    bucketCopy.batchRun(sourceBucket, srcKey, targetBucket, keyPrefix + tarKey, force, retryCount) :
                    bucketCopy.run(sourceBucket, srcKey, targetBucket, keyPrefix + tarKey, force, retryCount);
            if (bucketCopyResult != null) fileReaderAndWriterMap.writeSuccess(bucketCopyResult);
        } catch (QiniuException e) {
            if (!e.response.needRetry()) qiniuException = e;
            if (batch) fileReaderAndWriterMap.writeErrorOrNull(bucketCopy.getBatchOps() + "\t" + e.error());
            else fileReaderAndWriterMap.writeErrorOrNull(sourceBucket + "\t" + srcKey + "\t" + targetBucket + "\t" + tarKey + "\t" + e.error());
            e.response.close();
        }
    }

    public void processFile(String fileInfoStr, int retryCount, boolean batch) {
        JsonObject fileInfo = JSONConvertUtils.toJsonObject(fileInfoStr);
        String key = fileInfo.get("key").getAsString();
        bucketChangeTypeResult(srcBucket, key, tarBucket, key, false, retryCount, batch);
    }

    public void closeResource() {
        fileReaderAndWriterMap.closeWriter();
        if (bucketCopy != null)
            bucketCopy.closeBucketManager();
    }
}