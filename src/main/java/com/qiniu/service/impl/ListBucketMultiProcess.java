package com.qiniu.service.impl;

import com.google.gson.JsonObject;
import com.qiniu.common.*;
import com.qiniu.http.Response;
import com.qiniu.interfaces.IBucketProcess;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.service.oss.ListBucketProcessor;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.JSONConvertUtils;
import com.qiniu.util.StringUtils;
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

    public ListBucketMultiProcess(QiniuAuth auth, Configuration configuration, String bucket, IOssFileProcess iOssFileProcessor,
                                    FileReaderAndWriterMap fileReaderAndWriterMap, int threadNums) {
        this.iOssFileProcessor = iOssFileProcessor;
        this.bucketManager = new QiniuBucketManager(auth, configuration);
        this.bucket = bucket;
        this.fileReaderAndWriterMap = fileReaderAndWriterMap;
        this.listBucketProcessor = ListBucketProcessor.getChangeStatusProcessor(auth, configuration, fileReaderAndWriterMap);
        this.threadNums = threadNums;
    }

    public void processBucket() {
        doMultiList("", false);
    }

    public void processBucketV2(boolean withParallel) {
        doMultiList("v2", withParallel);
    }

    private Map<String, Integer> getDelimitedFileMap(String version) {
        List<String> prefixArray = new ArrayList<>();
        List<String> prefixs = Arrays.asList("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".split(""));
        prefixArray.add(0, "");
        prefixArray.addAll(prefixs);
        Map<String, Integer> delimitedFileMap = new LinkedHashMap<>();

        for (int i = 0; i < prefixArray.size(); i++) {
            FileListing fileListing;
            String fileInfoStr = "";
            try {
                if ("v2".equals(version)) {
                    Response response = listBucketProcessor.listV2(bucket, prefixArray.get(i), "", null, 1);
                    fileInfoStr = listBucketProcessor.getFileInfoV2(bucket, response.bodyString());
                } else {
                    fileListing = bucketManager.listFiles(bucket, prefixArray.get(i), null, 1, null);
                    fileInfoStr = listBucketProcessor.getFileListingInfo(bucket, fileListing, 0);
                }
            } catch (QiniuException e) {
                fileReaderAndWriterMap.writeErrorAndNull(bucket + "\t" + prefixArray.get(i) + "\t" + 1 + "\t" + "{\"msg\":\"" + e.error() + "\"}");
                continue;
            } catch (QiniuSuitsException e) {
                fileReaderAndWriterMap.writeErrorAndNull(bucket + "\t" + prefixArray.get(i) + "\t" + 1 + "\t" + e.getMessage());
                continue;
            }

            JsonObject json = JSONConvertUtils.toJson(fileInfoStr);
            String fileKey = json.get("key").getAsString();
            int fileType = json.get("type").getAsInt();
            if (delimitedFileMap.keySet().contains(fileKey)) {
                continue;
            }
            fileReaderAndWriterMap.writeSuccess(fileInfoStr);
            if (iOssFileProcessor != null) {
                iOssFileProcessor.processFile(fileInfoStr);
            }
            delimitedFileMap.put(fileKey, fileType);
        }

        // 原 bucketManager 实现中没有关闭单个请求的 response，修改实现使用类成员，使用完后统一关闭
        if (bucketManager != null)
            bucketManager.closeResponse();
        return delimitedFileMap;
    }

    private void doMultiList(String version, boolean withParallel) {

        Map<String, Integer> delimitedFileMap = getDelimitedFileMap(version);
        ExecutorService executorPool = Executors.newFixedThreadPool(threadNums);
        List<String> firstKeyList = new ArrayList<>(delimitedFileMap.keySet());

        for (int i = 0; i < firstKeyList.size(); i++) {
            String endFileKey = i == firstKeyList.size() - 1 ? "" : firstKeyList.get(i + 1);
            String startMarker = UrlSafeBase64.encodeToString("{\"c\":" + delimitedFileMap.get(firstKeyList.get(i))
                    + ",\"k\":\"" + firstKeyList.get(i) + "\"}");

            executorPool.execute(() -> {
                String marker = startMarker;
                while (!StringUtils.isNullOrEmpty(marker)) {
                    marker = "v2".equals(version) ?
                            listBucketProcessor.doFileListV2(bucket, "", "", marker, endFileKey, 10000,
                                    iOssFileProcessor, withParallel) :
                            listBucketProcessor.doFileList(bucket, null, marker, endFileKey, 1000, iOssFileProcessor);
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

    public void closeResource() {
        if (listBucketProcessor != null)
            listBucketProcessor.closeBucketManager();
    }
}