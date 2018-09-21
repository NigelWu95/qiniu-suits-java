package com.qiniu.service.oss;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuBucketManager;
import com.qiniu.common.QiniuBucketManager.*;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.StringMap;

import java.io.*;

public class ListBucketProcessor {

    private QiniuAuth auth;
    private QiniuBucketManager bucketManager;
    private Client client;
    private FileReaderAndWriterMap targetFileReaderAndWriterMap;

    private static volatile ListBucketProcessor listBucketProcessor = null;

    public ListBucketProcessor(QiniuAuth auth, Configuration configuration, FileReaderAndWriterMap targetFileReaderAndWriterMap) {
        this.auth = auth;
        this.bucketManager = new QiniuBucketManager(auth, configuration);
        this.client = new Client();
        this.targetFileReaderAndWriterMap = targetFileReaderAndWriterMap;
    }

    public static ListBucketProcessor getChangeStatusProcessor(QiniuAuth auth, Configuration configuration, FileReaderAndWriterMap targetFileReaderAndWriterMap) {
        if (listBucketProcessor == null) {
            synchronized (ListBucketProcessor.class) {
                if (listBucketProcessor == null) {
                    listBucketProcessor = new ListBucketProcessor(auth, configuration, targetFileReaderAndWriterMap);
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

    /*
    v2 的 list 接口，通过文本流的方式返回文件信息，接收到响应后通过 java8 的流来处理。
     */
    public void doListV2(String bucket, String prefix, String delimiter, String marker, int limit, IOssFileProcess iOssFileProcessor) {

        String url = "http://rsf.qbox.me/v2/list?bucket=" + bucket + "&prefix=" + prefix + "&delimiter=" + delimiter
                + "&limit=" + limit + "&marker=" + marker;
        String authorization = "QBox " + auth.signRequest(url, null, null);
        StringMap headers = new StringMap().put("Authorization", authorization);
        Response response = null;

        try {
            response = client.post(url, null, headers, null);
            InputStream inputStream = new BufferedInputStream(response.bodyStream());
            Reader reader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(reader);
            bufferedReader.lines().forEach(lineJson -> System.out.println(lineJson));
            inputStream.close();
            reader.close();
            bufferedReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    public void closeBucketManager() {
        if (bucketManager != null)
            bucketManager.closeResponse();
    }
}