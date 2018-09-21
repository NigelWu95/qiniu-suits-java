package com.qiniu.service.impl;

import com.qiniu.common.*;
import com.qiniu.interfaces.IBucketProcess;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.service.oss.ListBucketProcessor;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.UrlSafeBase64;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ListBucketMultiProcess implements IBucketProcess {

    private ListBucketProcessor listBucketProcessor;
    private QiniuBucketManager bucketManager;
    private String bucket;
    private IOssFileProcess iOssFileProcessor;
    private FileReaderAndWriterMap fileReaderAndWriterMap;
    private int threadNums;

    public ListBucketMultiProcess(QiniuBucketManager bucketManager, String bucket, IOssFileProcess iOssFileProcessor,
                                  FileReaderAndWriterMap fileReaderAndWriterMap, int threadNums) {
        this.iOssFileProcessor = iOssFileProcessor;
        this.bucketManager = bucketManager;
        this.bucket = bucket;
        this.fileReaderAndWriterMap = fileReaderAndWriterMap;
        this.listBucketProcessor = ListBucketProcessor.getChangeStatusProcessor(bucketManager, fileReaderAndWriterMap);
        this.threadNums = threadNums;
    }

    public void processBucket() {
        doMultiList();
    }

    public void doMultiList() {
        List<String> prefixArray = new ArrayList<>();
        List<String> prefixs = Arrays.asList("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".split(""));
        prefixArray.add(0, "");
        prefixArray.addAll(prefixs);

        Map<String, String> firstKeyMap = new LinkedHashMap<>();
        for (int i = 0; i < prefixArray.size(); i++) {
            FileListing fileListing = null;
            try {
                fileListing = bucketManager.listFiles(bucket, prefixArray.get(i), null, 1, null);
                FileInfo[] items = fileListing.items;

                for (FileInfo fileInfo : items) {
                    if (firstKeyMap.keySet().contains(fileInfo.key)) {
                        continue;
                    }

                    fileReaderAndWriterMap.writeSuccess(fileInfo.key + "\t" + fileInfo.fsize + "\t" + fileInfo.hash
                            + "\t" + fileInfo.putTime+ "\t" + fileInfo.mimeType+ "\t" + fileInfo.type + "\t" + fileInfo.endUser);
                    if (iOssFileProcessor != null) {
                        iOssFileProcessor.processFile(fileInfo);
                    }
                    firstKeyMap.put(fileInfo.key, String.valueOf(fileInfo.type));
                }
            } catch (QiniuException e) {
                fileReaderAndWriterMap.writeErrorAndNull(bucket + "\t" + prefixArray.get(i) + "\t" + 1 + "\t" + e.code() + "\t" + e.error());
            }
        }

        ExecutorService executorPool = Executors.newFixedThreadPool(threadNums);
        List<String> firstKeyList = new ArrayList<>(firstKeyMap.keySet());

        for (int i = 0; i < firstKeyList.size(); i++) {
            String endFileKey = i == firstKeyList.size() - 1 ? "" : firstKeyList.get(i + 1);
            String startMarker = UrlSafeBase64.encodeToString("{\"c\":" + firstKeyMap.get(firstKeyList.get(i)) + ",\"k\":\"" + firstKeyList.get(i) + "\"}");

            executorPool.execute(new Runnable() {
                public void run() {
                    String marker = startMarker;
                    while (marker != null) {
                        marker = listBucketProcessor.doFileList(bucket, null, marker, endFileKey, 1000, iOssFileProcessor);
                    }
                }
            });
        }

        executorPool.shutdown();

        try {
            while (!executorPool.isTerminated()) {
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        if (iOssFileProcessor != null) {
            iOssFileProcessor.close();
        }
    }
}