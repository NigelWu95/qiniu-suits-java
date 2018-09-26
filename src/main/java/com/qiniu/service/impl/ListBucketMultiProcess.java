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

    private Map<String, Integer[]> getDelimitedFileMap(String version, boolean secondLevel) {
        List<String> prefixList = new ArrayList<>();
        List<String> prefix = Arrays.asList("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".split(""));
        prefixList.add("");
        prefixList.addAll(prefix);
        Map<String, Integer[]> delimitedFileMap;

        if (secondLevel) {
            delimitedFileMap = listByPrefix(prefixList, version, false);
            prefixList = getSecondFilePrefix(prefixList, delimitedFileMap);
            delimitedFileMap.putAll(listByPrefix(prefixList, version, true));
        } else {
            delimitedFileMap = listByPrefix(prefixList, version, true);
        }

        // 原 bucketManager 实现中没有关闭单个请求的 response，修改实现使用类成员，使用完后统一关闭
        if (bucketManager != null)
            bucketManager.closeResponse();
        return delimitedFileMap;
    }

    private Map<String, Integer[]> listByPrefix(List<String> prefixList, String version, boolean doProcess) {
        Map<String, Integer[]> delimitedFileMap = new HashMap<>();

        for (int i = 0; i < prefixList.size(); i++) {
            String[] fileInfoAndMarker;
            try {
                if ("v2".equals(version)) {
                    fileInfoAndMarker = listBucketProcessor.getFileInfoV2AndMarker(bucket, prefixList.get(i), null,
                            null, 1, 3);
                } else {
                    fileInfoAndMarker = listBucketProcessor.getFirstFileInfoAndMarker(bucket, prefixList.get(i),
                            null, null, 1, 0, 3);
                }
            } catch (QiniuSuitsException e) {
                fileReaderAndWriterMap.writeErrorAndNull(bucket + "\t" + prefixList.get(i) + "\t" + 1 + "\t" + e.getMessage());
                continue;
            }

            JsonObject json = JSONConvertUtils.toJson(fileInfoAndMarker[0]);

            if (json.keySet().contains("dir")) {
                fileReaderAndWriterMap.writeOther(fileInfoAndMarker[0]);
                continue;
            }

            String fileKey = json.get("key").getAsString();
            int fileType = json.get("type").getAsInt();

            if (doProcess && !delimitedFileMap.keySet().contains(fileKey)) {
                fileReaderAndWriterMap.writeSuccess(fileInfoAndMarker[0]);
                if (iOssFileProcessor != null) {
                    iOssFileProcessor.processFile(fileInfoAndMarker[0], 3);
                }
            }

            Integer[] keyAttribute = new Integer[]{fileType, 1};

            if (StringUtils.isNullOrEmpty(fileInfoAndMarker[1]))
                keyAttribute[1] = 0;

            delimitedFileMap.put(fileKey, keyAttribute);
        }

        return delimitedFileMap;
    }

    private List<String> getSecondFilePrefix(List<String> prefixList, Map<String, Integer[]> delimitedFileMap) {
        List<String> firstKeyList = new ArrayList<>(delimitedFileMap.keySet());
        List<String> secondPrefixList = new ArrayList<>();

        for (String firstKey : firstKeyList) {
            String firstPrefix = firstKey.substring(0, 1);
            if (delimitedFileMap.get(firstKey)[1] == 0) {
                secondPrefixList.add(firstPrefix);
                continue;
            }
            for (String secondPrefix : prefixList) {
                secondPrefixList.add(firstPrefix + secondPrefix);
            }
        }

        return secondPrefixList;
    }

    private void doMultiList(String version, boolean withParallel, boolean secondLevel) {

        Map<String, Integer[]> delimitedFileMap = getDelimitedFileMap(version, secondLevel);
        ExecutorService executorPool = Executors.newFixedThreadPool(threadNums);
        List<String> firstKeyList = new ArrayList<>(delimitedFileMap.keySet());
        Collections.sort(firstKeyList);

        for (int i = 0; i < firstKeyList.size(); i++) {
            String endFileKey = i == firstKeyList.size() - 1 ? "" : firstKeyList.get(i + 1);
            String startMarker = UrlSafeBase64.encodeToString("{\"c\":" + delimitedFileMap.get(firstKeyList.get(i))[0]
                    + ",\"k\":\"" + firstKeyList.get(i) + "\"}");

            executorPool.execute(() -> {
                String marker = startMarker;
                while (!StringUtils.isNullOrEmpty(marker)) {
                    marker = "v2".equals(version) ?
                            listBucketProcessor.doFileListV2(bucket, "", "", marker, 1000, endFileKey,
                                    iOssFileProcessor, withParallel, 3) :
                            listBucketProcessor.doFileList(bucket, null, null, marker, 1000, endFileKey
                                    , iOssFileProcessor, 3);
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