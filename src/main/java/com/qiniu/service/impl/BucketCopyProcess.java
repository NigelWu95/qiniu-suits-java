package com.qiniu.service.impl;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.service.oss.BucketCopy;
import com.qiniu.storage.Configuration;
import com.qiniu.util.StringUtils;

import java.io.IOException;
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
        this.bucketCopy = new BucketCopy(auth, configuration);
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

    public void processFile(String fileKey, int retryCount) {

        try {
            String result = bucketCopy.run(srcBucket, fileKey, tarBucket, fileKey, keyPrefix, false, retryCount);
            if (!StringUtils.isNullOrEmpty(result)) fileReaderAndWriterMap.writeSuccess(result);
        } catch (QiniuException e) {
            if (!e.response.needRetry()) qiniuException = e;
            fileReaderAndWriterMap.writeErrorOrNull(srcBucket + "\t" + fileKey + "\t" + tarBucket + "\t" + fileKey + "\t" + e.error());
            e.response.close();
        }
    }

    public void processFile(List<String> keyList, int retryCount) {

        int times = keyList.size()/1000 + 1;
        for (int i = 0; i < times; i++) {
            List<String> processList = keyList.subList(1000 * i, i == times - 1 ? keyList.size() : 1000 * (i + 1));
            try {
                String result = bucketCopy.batchRun(srcBucket, tarBucket, processList, keyPrefix, false, retryCount);
                if (!StringUtils.isNullOrEmpty(result)) fileReaderAndWriterMap.writeSuccess(result);
            } catch (QiniuException e) {
                if (!e.response.needRetry()) qiniuException = e;
                fileReaderAndWriterMap.writeErrorOrNull(srcBucket + "\t" + tarBucket + "\t" + processList + "\t" + false + "\t" + e.error());
                e.response.close();
            }
        }
    }

    public void closeResource() {
        fileReaderAndWriterMap.closeWriter();
        if (bucketCopy != null)
            bucketCopy.closeBucketManager();
    }
}