package com.qiniu.service.oss;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuBucketManager;
import com.qiniu.common.QiniuBucketManager.*;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;

public class ListBucketProcessor {

    private QiniuBucketManager bucketManager;
    private FileReaderAndWriterMap targetFileReaderAndWriterMap;

    private static volatile ListBucketProcessor listBucketProcessor = null;

    public ListBucketProcessor(QiniuBucketManager bucketManager, FileReaderAndWriterMap targetFileReaderAndWriterMap) {
        this.bucketManager = bucketManager;
        this.targetFileReaderAndWriterMap = targetFileReaderAndWriterMap;
    }

    public static ListBucketProcessor getChangeStatusProcessor(QiniuBucketManager bucketManager, FileReaderAndWriterMap targetFileReaderAndWriterMap) {
        if (listBucketProcessor == null) {
            synchronized (ListBucketProcessor.class) {
                if (listBucketProcessor == null) {
                    listBucketProcessor = new ListBucketProcessor(bucketManager, targetFileReaderAndWriterMap);
                }
            }
        }
        return listBucketProcessor;
    }

    public void doFileList(String bucket, String prefix, int limit, IOssFileProcess iOssFileProcessor) {

        try {
            FileListing fileListing = bucketManager.listFiles(bucket, prefix, null, limit, null);
            FileInfo[] items = fileListing.items;

            for (FileInfo fileInfo : items) {
                targetFileReaderAndWriterMap.writeSuccess(fileInfo.endUser + "\t" + fileInfo.hash + "\t"
                        + fileInfo.key + "\t" + fileInfo.mimeType + "\t" + fileInfo.fsize + "\t" + fileInfo.putTime
                        + "\t" + fileInfo.type);
                iOssFileProcessor.processFile(fileInfo);
            }
        } catch (QiniuException e) {
            targetFileReaderAndWriterMap.writeErrorAndNull(bucket + "\t" + prefix + "\t" + limit + "\t" + e.code() + "\t" + e.error());
        }
    }

    public void doFileIteratorList(String bucket, String prefix, int limit, IOssFileProcess iOssFileProcessor) {

        FileListIterator fileListIterator = bucketManager.createFileListIterator(bucket, prefix, limit, null);

        while (fileListIterator.hasNext()) {
            FileInfo[] items = fileListIterator.next();

            for (FileInfo item : items) {
                targetFileReaderAndWriterMap.writeSuccess(item.endUser + "\t" + item.hash + "\t"
                        + item.key + "\t" + item.mimeType + "\t" + item.fsize + "\t" + item.putTime
                        + "\t" + item.type);
                iOssFileProcessor.processFile(item);
            }
        }
    }

}