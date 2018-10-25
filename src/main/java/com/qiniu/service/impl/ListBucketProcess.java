package com.qiniu.service.impl;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.qiniu.common.*;
import com.qiniu.http.Response;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.model.ListResult;
import com.qiniu.model.ListV2Line;
import com.qiniu.sdk.QiniuAuth;
import com.qiniu.service.oss.ListBucket;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class ListBucketProcess {

    private QiniuAuth auth;
    private Configuration configuration;
    private String bucket;
    private int unitLen;
    private int version;
    private String resultFileDir;
    private String customPrefix;
    private List<String> antiPrefix;
    private ListFileFilter listFileFilter;
    private ListFileAntiFilter listFileAntiFilter;
    private boolean checkListFileFilter;
    private boolean checkListFileAntiFilter;
    private List<String> originPrefixList = Arrays.asList(
            " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
            .split(""));

    public ListBucketProcess(QiniuAuth auth, Configuration configuration, String bucket, int unitLen, int version,
                             String resultFileDir, String customPrefix, List<String> antiPrefix) {
        this.auth = auth;
        this.configuration = configuration;
        this.bucket = bucket;
        this.unitLen = unitLen;
        this.version = version;
        this.resultFileDir = resultFileDir;
        this.customPrefix = customPrefix;
        this.antiPrefix = antiPrefix;
    }

    public void setFilter(ListFileFilter listFileFilter, ListFileAntiFilter listFileAntiFilter) {
        this.listFileFilter = listFileFilter;
        this.listFileAntiFilter = listFileAntiFilter;
        this.checkListFileFilter = ListFileFilterUtils.checkListFileFilter(listFileFilter);
        this.checkListFileAntiFilter = ListFileFilterUtils.checkListFileAntiFilter(listFileAntiFilter);
    }

    private List<FileInfo> filterFileInfo(List<FileInfo> fileInfoList) {

        if (checkListFileFilter && checkListFileAntiFilter) {
            return fileInfoList.parallelStream()
                    .filter(fileInfo -> listFileFilter.doFileFilter(fileInfo) && listFileAntiFilter.doFileAntiFilter(fileInfo))
                    .collect(Collectors.toList());
        } else if (checkListFileFilter) {
            return fileInfoList.parallelStream()
                    .filter(fileInfo -> listFileFilter.doFileFilter(fileInfo))
                    .collect(Collectors.toList());
        } else if (checkListFileAntiFilter) {
            return fileInfoList.parallelStream()
                    .filter(fileInfo -> listFileAntiFilter.doFileAntiFilter(fileInfo))
                    .collect(Collectors.toList());
        } else return fileInfoList;
    }

    private void writeAndProcess(List<FileInfo> fileInfoList, String endFilePrefix, FileReaderAndWriterMap fileMap,
                                 IOssFileProcess iOssFileProcessor, boolean processBatch, int retryCount,
                                 Queue<QiniuException> exceptionQueue) throws QiniuException {

        if (fileInfoList == null || fileInfoList.size() == 0) return;
        fileInfoList = fileInfoList.parallelStream()
                .filter(fileInfo ->  StringUtils.isNullOrEmpty(endFilePrefix) ? fileInfo != null :
                        fileInfo != null && endFilePrefix.compareTo(fileInfo.key) > 0)
                .collect(Collectors.toList());
        if (fileInfoList == null || fileInfoList.size() == 0) return;
        // 如果 list 不为空，将完整的列表先写入。
        if (fileMap != null) fileMap.writeSuccess(
                String.join("\n", fileInfoList.parallelStream()
                        .map(JsonConvertUtils::toJsonWithoutUrlEscape)
                        .collect(Collectors.toList()))
        );

        if (checkListFileFilter || checkListFileAntiFilter) {
            fileInfoList = filterFileInfo(fileInfoList);
            if (fileInfoList == null || fileInfoList.size() == 0) return;
            // 如果有过滤条件的情况下，将过滤之后的结果单独写入到 other 文件中。
            if (fileMap != null) fileMap.writeOther(String.join("\n", fileInfoList.parallelStream()
                    .filter(Objects::nonNull)
                    .map(JsonConvertUtils::toJsonWithoutUrlEscape)
                    .collect(Collectors.toList()))
            );
        }

        if (iOssFileProcessor != null) {
            if (processBatch) {
                iOssFileProcessor.processFile(fileInfoList.parallelStream()
                        .map(fileInfo -> fileInfo.key)
                        .collect(Collectors.toList()), retryCount);
            } else {
                fileInfoList.parallelStream()
                        .forEach(fileInfo -> iOssFileProcessor.processFile(fileInfo.key, retryCount));
            }

            if (iOssFileProcessor.qiniuException() != null && iOssFileProcessor.qiniuException().code() > 400 &&
                    exceptionQueue != null)
                exceptionQueue.add(iOssFileProcessor.qiniuException());
        }

        if (exceptionQueue != null) {
            QiniuException qiniuException = exceptionQueue.poll();
            if (qiniuException != null) throw qiniuException;
        }
    }

    public ListV2Line getItemByList2Line(String line) {

        ListV2Line listV2Line = new ListV2Line();
        if (!StringUtils.isNullOrEmpty(line)) {
            JsonObject json = JsonConvertUtils.toJsonObject(line);
            JsonElement item = json.get("item");
            JsonElement marker = json.get("marker");
            if (item != null && !(item instanceof JsonNull)) {
                listV2Line.fileInfo = JsonConvertUtils.fromJson(item, FileInfo.class);
            }
            if (marker != null && !(marker instanceof JsonNull)) {
                listV2Line.marker = marker.getAsString();
            }
        }
        return listV2Line;
    }

    public ListResult getListResult(Response response, int version) throws QiniuException {

        ListResult listResult = new ListResult();
        if (response != null) {
            if (version == 1) {
                FileListing fileListing = response.jsonToObject(FileListing.class);
                if (fileListing != null) {
                    FileInfo[] items = fileListing.items;
                    listResult.nextMarker = fileListing.marker;
                    if (items.length > 0) listResult.fileInfoList = Arrays.asList(items);
                }
            } else if (version == 2) {
                InputStream inputStream = new BufferedInputStream(response.bodyStream());
                Reader reader = new InputStreamReader(inputStream);
                BufferedReader bufferedReader = new BufferedReader(reader);
                List<String> lineList = bufferedReader.lines().collect(Collectors.toList());
                try {
                    bufferedReader.close();
                    reader.close();
                    inputStream.close();
                } catch (IOException e) {}
                if (lineList != null && lineList.size() > 0) {
                    String line = lineList.get(lineList.size() - 1);
                    ListV2Line listV2Line = getItemByList2Line(line);
                    listResult.fileInfoList = lineList.parallelStream()
                            .filter(everyLine -> !StringUtils.isNullOrEmpty(everyLine))
                            .map(everyLine -> getItemByList2Line(everyLine).fileInfo)
                            .collect(Collectors.toList());
                    listResult.nextMarker = listV2Line.marker;
                }
            }
        }

        return listResult;
    }

    private List<ListResult> listByPrefixList(ListBucket listBucket, List<String> prefixList, int retryCount) throws IOException {
        Queue<QiniuException> exceptionQueue = new ConcurrentLinkedQueue<>();
        FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap();
        fileMap.initWriter(resultFileDir, "list");
        List<ListResult> listResultList = prefixList.parallelStream()
                .map(prefix -> {
                    Response response = null;
                    ListResult listResult = new ListResult();
                    try {
                        response = listBucket.run(bucket, prefix, null, null, unitLen, retryCount, version);
                        listResult = getListResult(response, version);
                        listResult.commonPrefix = prefix;
                    } catch (QiniuException e) {
                        fileMap.writeErrorOrNull(bucket + "\t" + prefix + "\t" + e.error());
                        if (e.code() > 400) exceptionQueue.add(e);
                    } finally { if (response != null) response.close(); }
                    return listResult;
                })
                .filter(ListResult::isValid)
                .collect(Collectors.toList());
        QiniuException qiniuException = exceptionQueue.poll();
        if (qiniuException != null) throw qiniuException;
        return listResultList;
    }

    public void loopList(ListBucket listBucket, String prefix, String endFilePrefix, String marker,
                                 FileReaderAndWriterMap fileMap, IOssFileProcess processor, boolean processBatch) {
        while (!"".equals(marker)) {
            try {
                Response response = listBucket.run(bucket, prefix, "", marker, unitLen, 3, version);
                ListResult listResult = getListResult(response, version);
                response.close();
                List<FileInfo> fileInfoList = listResult.fileInfoList;
                writeAndProcess(fileInfoList, endFilePrefix, fileMap, processor, processBatch, 3, null);
                marker = (!StringUtils.isNullOrEmpty(endFilePrefix) && fileInfoList.parallelStream()
                        .anyMatch(fileInfo -> fileInfo != null && endFilePrefix.compareTo(fileInfo.key) < 0) ?
                        "" : listResult.nextMarker);
            } catch (IOException e) {
                fileMap.writeErrorOrNull(bucket + "\t" + prefix + endFilePrefix + "\t" + marker + "\t" + unitLen
                        + "\t" + e.getMessage());
            }
        }
    }

    private void listWith2Prefix(ExecutorService executorPool, List<String> prefixList, boolean strictPrefix,
                               IOssFileProcess iOssFileProcessor, boolean processBatch, int retryCount)
            throws IOException, CloneNotSupportedException {

        ListBucket listBucketLevel1 = new ListBucket(auth, configuration);
        List<ListResult> listResultList = listByPrefixList(listBucketLevel1, prefixList, retryCount);
        for (int i = strictPrefix ? 0 : -1; i < listResultList.size(); i++) {
            int finalI = i;
            FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap(i + 1);
            fileMap.initWriter(resultFileDir, "list");
            IOssFileProcess processor = iOssFileProcessor != null ? iOssFileProcessor.clone() : null;
            writeAndProcess(i > -1 ? listResultList.get(i).fileInfoList : null, "", fileMap, processor, processBatch,
                    retryCount, new ConcurrentLinkedQueue<>());
            executorPool.execute(() -> {
                String endFilePrefix = "";
                String prefix = "";
                String marker = null;
                if (finalI < listResultList.size() -1 && finalI > -1) {
                    prefix = listResultList.get(finalI).commonPrefix;
                    marker = listResultList.get(finalI).nextMarker;
                } else {
                    if (finalI == -1) endFilePrefix = listResultList.get(0).commonPrefix;
                    else marker = listResultList.get(finalI).nextMarker;
                    if (strictPrefix) prefix = customPrefix;
                }
                ListBucket listBucket = new ListBucket(auth, configuration);
                loopList(listBucket, prefix, endFilePrefix, marker, fileMap, processor, processBatch);
                listBucket.closeBucketManager();
                if (processor != null) processor.closeResource();
                fileMap.closeWriter();
            });
        }
    }

    public void processBucket(int maxThreads, int level, IOssFileProcess iOssFileProcessor, boolean processBatch, int retryCount)
            throws IOException, CloneNotSupportedException {
        boolean strictPrefix = !StringUtils.isNullOrEmpty(customPrefix);
        List<String> prefixList = originPrefixList.parallelStream()
                .filter(originPrefix -> !antiPrefix.contains(originPrefix))
                .map(prefix -> strictPrefix ? customPrefix + prefix : prefix)
                .collect(Collectors.toList());
        int runningThreads = strictPrefix ? prefixList.size() : prefixList.size() + 2;
        ExecutorService executorPool = Executors.newFixedThreadPool(runningThreads < maxThreads ? runningThreads : maxThreads);

        if (level == 2) {
            listWith2Prefix(executorPool, prefixList, strictPrefix, iOssFileProcessor, processBatch, retryCount);
        } else {
            for (int i = 0; i < prefixList.size(); i++) {
                String prefix = prefixList.get(i);
                FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap(i + 1);
                fileMap.initWriter(resultFileDir, "list");
                IOssFileProcess processor = iOssFileProcessor != null ? iOssFileProcessor.clone() : null;
                executorPool.execute(() -> {
                    ListBucket listBucket = new ListBucket(auth, configuration);
                    loopList(listBucket, prefix, "", null, fileMap, processor, processBatch);
                    listBucket.closeBucketManager();
                    if (processor != null) processor.closeResource();
                    fileMap.closeWriter();
                });
            }
        }

        executorPool.shutdown();
        try {
            while (!executorPool.isTerminated())
                Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}