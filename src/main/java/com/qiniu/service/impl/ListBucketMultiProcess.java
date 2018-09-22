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

    public void processBucket(boolean secondLevel) {
        doMultiList("", false, secondLevel);
    }

    public void processBucketV2(boolean withParallel, boolean secondLevel) {
        doMultiList("v2", withParallel, secondLevel);
    }

    private Map<String, Integer> getDelimitedFileMap(String version, boolean secondLevel) {
        List<String> prefixArray = new ArrayList<>();
        List<String> prefixs = Arrays.asList("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".split(""));
        prefixArray.add("");
        prefixArray.addAll(prefixs);
        Map<String, Integer> delimitedFileMap;

        if (secondLevel) {
            delimitedFileMap = listByPrefix(prefixArray, version, false);
            System.out.println("1 " + prefixArray);
            prefixArray = getSecondDelimitedFileMap(prefixArray, new ArrayList<>(delimitedFileMap.keySet()));
            System.out.println("2 " + prefixArray);
            prefixArray.add("");
            System.out.println("3 " + prefixArray);
            delimitedFileMap.putAll(listByPrefix(prefixArray, version, true));
            System.out.println("4 " + delimitedFileMap.size());
        } else {
            delimitedFileMap = listByPrefix(prefixArray, version, true);
            System.out.println("1 " + delimitedFileMap.size());
        }

        // 原 bucketManager 实现中没有关闭单个请求的 response，修改实现使用类成员，使用完后统一关闭
        if (bucketManager != null)
            bucketManager.closeResponse();
        return delimitedFileMap;
    }

    private Map<String, Integer> listByPrefix(List<String> prefixArray, String version, boolean doProcess) {
        Map<String, Integer> delimitedFileMap = new LinkedHashMap<>();

        for (int i = 0; i < prefixArray.size(); i++) {
            FileListing fileListing;
            String[] fileInfoAndMarker;
            try {
                if ("v2".equals(version)) {
                    Response response = listBucketProcessor.listV2(bucket, prefixArray.get(i), "", null, 1);
                    fileInfoAndMarker = listBucketProcessor.getFileInfoV2AndMarker(bucket, response.bodyString());
                } else {
                    fileListing = bucketManager.listFiles(bucket, prefixArray.get(i), null, 1, null);
                    fileInfoAndMarker = listBucketProcessor.getFirstFileInfoAndMarker(bucket, fileListing, 0);
                }
            } catch (QiniuException e) {
                fileReaderAndWriterMap.writeErrorAndNull(bucket + "\t" + prefixArray.get(i) + "\t" + 1 + "\t" + "{\"msg\":\"" + e.error() + "\"}");
                continue;
            } catch (QiniuSuitsException e) {
                fileReaderAndWriterMap.writeErrorAndNull(bucket + "\t" + prefixArray.get(i) + "\t" + 1 + "\t" + e.getMessage());
                continue;
            }

            JsonObject json = JSONConvertUtils.toJson(fileInfoAndMarker[0]);
            String fileKey = json.get("key").getAsString();
            int fileType = json.get("type").getAsInt();

            if (doProcess && !delimitedFileMap.keySet().contains(fileKey)) {
                fileReaderAndWriterMap.writeSuccess(fileInfoAndMarker[0]);
                if (iOssFileProcessor != null) {
                    iOssFileProcessor.processFile(fileInfoAndMarker[0]);
                }
            }

//            if (StringUtils.isNullOrEmpty(fileInfoAndMarker[1]))
//                continue;
            delimitedFileMap.put(fileKey, fileType);
        }

        return delimitedFileMap;
    }

    private List<String> getSecondDelimitedFileMap(List<String> prefixArray, List<String> firstKeyList) {

        List<String> secondPrefixArray = new ArrayList<>();

        for (int i = 0; i < firstKeyList.size(); i++) {
            String firstPrefix = firstKeyList.get(i).substring(0, 1);
            for (String secondPrefix : prefixArray) {
                secondPrefixArray.add(firstPrefix + secondPrefix);
            }
        }

        return secondPrefixArray;
    }

    private void doMultiList(String version, boolean withParallel, boolean secondLevel) {

        Map<String, Integer> delimitedFileMap = getDelimitedFileMap(version, secondLevel);
        ExecutorService executorPool = Executors.newFixedThreadPool(threadNums);
        List<String> firstKeyList = new ArrayList<>(delimitedFileMap.keySet());
        Collections.sort(firstKeyList);

        System.out.println(firstKeyList);

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