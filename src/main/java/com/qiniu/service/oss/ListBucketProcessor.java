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

    /*
    单次列举，可以传递 marker 和 limit 参数，通常采用此方法进行并发处理
     */
    public String doFileList(String bucket, String prefix, String marker, String endFile, int limit, IOssFileProcess iOssFileProcessor) {

        String endMarker = null;

        try {
            FileListing fileListing = bucketManager.listFiles(bucket, prefix, marker, limit, null);
            FileInfo[] items = fileListing.items;

            for (FileInfo fileInfo : items) {
                if (fileInfo.key.equals(endFile)) {
                    fileListing = null;
                    break;
                }
                targetFileReaderAndWriterMap.writeSuccess(fileInfo.key + "\t" + fileInfo.fsize + "\t" + fileInfo.hash
                        + "\t" + fileInfo.putTime+ "\t" + fileInfo.mimeType+ "\t" + fileInfo.type + "\t" + fileInfo.endUser);
                if (iOssFileProcessor != null) {
                    iOssFileProcessor.processFile(fileInfo);
                }
            }

            endMarker = fileListing == null ? null : fileListing.marker;
        } catch (QiniuException e) {
            targetFileReaderAndWriterMap.writeErrorAndNull(bucket + "\t" + prefix + "\t" + limit + "\t" + e.code() + "\t" + e.error());
        } finally {
            return endMarker;
        }
    }

    /*
    迭代器方式列举带 prefix 前缀的所有文件，直到列举完毕，limit 为单次列举的文件个数
     */
    public void doFileIteratorList(String bucket, String prefix, String endFile, int limit, IOssFileProcess iOssFileProcessor) {

        FileListIterator fileListIterator = bucketManager.createFileListIterator(bucket, prefix, limit, null);

        loop:while (fileListIterator.hasNext()) {
            FileInfo[] items = fileListIterator.next();

            for (FileInfo fileInfo : items) {
                if (fileInfo.key.equals(endFile)) {
                    break loop;
                }
                targetFileReaderAndWriterMap.writeSuccess(fileInfo.key + "\t" + fileInfo.fsize + "\t" + fileInfo.hash
                        + "\t" + fileInfo.putTime+ "\t" + fileInfo.mimeType+ "\t" + fileInfo.type + "\t" + fileInfo.endUser);
                if (iOssFileProcessor != null) {
                    iOssFileProcessor.processFile(fileInfo);
                }
            }
        }
    }

}