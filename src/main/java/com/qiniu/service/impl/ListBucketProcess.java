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
    private String resultFileDir;
    private ListFileFilter listFileFilter;
    private ListFileAntiFilter listFileAntiFilter;
    private boolean checkListFileFilter;
    private boolean checkListFileAntiFilter;

    public ListBucketProcess(QiniuAuth auth, Configuration configuration, String bucket, String resultFileDir) {
        this.auth = auth;
        this.configuration = configuration;
        this.bucket = bucket;
        this.resultFileDir = resultFileDir;
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
        if (!"".equals(endFilePrefix)) {
            fileInfoList = fileInfoList.parallelStream()
                            .filter(fileInfo -> fileInfo != null && endFilePrefix.compareTo(fileInfo.key) > 0)
                            .collect(Collectors.toList());
        }

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
                    if (items.length > 0) {
                        listResult.fileInfoList = Arrays.asList(items);
                    }
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

    private List<ListResult> listByPrefixList(ListBucket listBucket, List<String> prefixList, int unitLen, int version, FileReaderAndWriterMap fileMap,
                                        Queue<QiniuException> exceptionQueue, int retryCount) {

        return prefixList.parallelStream()
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
    }

    private void processDelimitedFileInfo(List<ListResult> listResultList, FileReaderAndWriterMap fileMap, IOssFileProcess iOssFileProcessor,
                                          boolean processBatch, int retryCount, Queue<QiniuException> exceptionQueue) throws QiniuException {
        List<FileInfo> fileInfoList = listResultList.parallelStream()
                .filter(listResult -> listResult.fileInfoList != null)
                .map(listResult -> listResult.fileInfoList)
                .reduce((fileInfoList1, fileInfoList2) -> {
                    fileInfoList1.addAll(fileInfoList2);
                    return fileInfoList1;
                }).get();
        writeAndProcess(fileInfoList, "", fileMap, iOssFileProcessor, processBatch, retryCount, exceptionQueue);
    }

    public Map<String, String> preListForDelimiter(int version, int unitLen, int level, String customPrefix, List<String> antiPrefix,
                                                   String resultPrefix, IOssFileProcess iOssFileProcessor, boolean processBatch,
                                                   int retryCount) throws IOException {
        Queue<QiniuException> exceptionQueue = new ConcurrentLinkedQueue<>();
        FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap();
        fileMap.initWriter(resultFileDir, resultPrefix);
        ListBucket listBucket = new ListBucket(auth, configuration);
        List<String> originPrefixList = Arrays.asList(" !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~".split(""));
        originPrefixList = originPrefixList.parallelStream()
                .filter(originPrefix -> !antiPrefix.contains(originPrefix))
                .collect(Collectors.toList());
        List<String> prefixList = !StringUtils.isNullOrEmpty(customPrefix) ? originPrefixList
                .parallelStream()
                .filter(originPrefix -> !antiPrefix.contains(originPrefix))
                .map(prefix -> customPrefix + prefix)
                .collect(Collectors.toList()) : originPrefixList;

        Map<String, String> delimiterMap = new HashMap<>();
        List<ListResult> listResultList = listByPrefixList(listBucket, prefixList, unitLen, version, fileMap, exceptionQueue, retryCount);
        if (level == 2) {
            if ("list".equals(resultPrefix)) processDelimitedFileInfo(listResultList.parallelStream()
                    .filter(listResult -> StringUtils.isNullOrEmpty(listResult.nextMarker))
                    .collect(Collectors.toList()), fileMap, iOssFileProcessor, processBatch, retryCount, exceptionQueue);
            List<String> delimiterList = listResultList.parallelStream()
                    .filter(listResult -> !StringUtils.isNullOrEmpty(listResult.nextMarker))
                    .map(listResult -> listResult.commonPrefix)
                    .collect(Collectors.toList());
            for (String firstPrefix : delimiterList) {
                List<String> secondPrefixList = originPrefixList.parallelStream().map(secondPrefix -> firstPrefix + secondPrefix).collect(Collectors.toList());
                listResultList = listByPrefixList(listBucket, secondPrefixList, unitLen, version, fileMap, exceptionQueue, retryCount);
                delimiterMap.putAll(listResultList.parallelStream()
                        .collect(Collectors.toMap(
                                listResult -> listResult.commonPrefix,
                                listResult -> listResult.nextMarker,
                                (listResult1, listResult2) -> listResult1
                        )));
                if ("list".equals(resultPrefix)) processDelimitedFileInfo(listResultList, fileMap, iOssFileProcessor, processBatch, retryCount, exceptionQueue);
            }
        } else {
            delimiterMap.putAll(listResultList.parallelStream()
                    .collect(Collectors.toMap(
                            listResult -> listResult.commonPrefix,
                            listResult -> listResult.nextMarker,
                            (listResult1, listResult2) -> listResult1
                    )));
            if ("list".equals(resultPrefix)) processDelimitedFileInfo(listResultList, fileMap, iOssFileProcessor, processBatch, retryCount, exceptionQueue);
        }
        listBucket.closeBucketManager();
        if ("delimiter".equals(resultPrefix)) fileMap.writeSuccess(String.join("\n", delimiterMap.keySet()));
        fileMap.closeWriter();

        return delimiterMap;
    }

    public void loopListByMarker(ListBucket listBucket, int unitLen, String prefix, String endFilePrefix, String marker,
                                 int version, FileReaderAndWriterMap fileMap, IOssFileProcess processor, boolean processBatch) {
        while (!StringUtils.isNullOrEmpty(marker)) {
            try {
                Response response = listBucket.run(bucket, prefix, "", marker.equals("null") ? "" : marker, unitLen, 3, version);
                ListResult listResult = getListResult(response, version);
                response.close();
                List<FileInfo> fileInfoList = listResult.fileInfoList;
                writeAndProcess(fileInfoList, endFilePrefix, fileMap, processor, processBatch, 3, null);
                marker = (!StringUtils.isNullOrEmpty(endFilePrefix) && fileInfoList.parallelStream()
                        .anyMatch(fileInfo -> fileInfo != null && endFilePrefix.compareTo(fileInfo.key) > 0) ?
                        null : listResult.nextMarker);
            } catch (IOException e) {
                fileMap.writeErrorOrNull(bucket + "\t" + prefix + endFilePrefix + "\t" + marker + "\t" + unitLen
                        + "\t" + e.getMessage());
            }
        }
    }

    public void processBucket(int version, int maxThreads, int level, int unitLen, String customPrefix,
                              List<String> antiPrefix, IOssFileProcess iOssFileProcessor, boolean processBatch)
            throws IOException, CloneNotSupportedException {

        System.out.println("list bucket concurrently running...");
        Map<String, String> delimiterMap = preListForDelimiter(version, unitLen, level, customPrefix, antiPrefix,"list",
                iOssFileProcessor, processBatch, 3);
        List<String> prefixList = new ArrayList<>(delimiterMap.keySet());
        Collections.sort(prefixList);
        boolean strictPrefix = !StringUtils.isNullOrEmpty(customPrefix);
        int runningThreads = strictPrefix ? delimiterMap.size() : delimiterMap.size() + 1;
        runningThreads = runningThreads < maxThreads ? runningThreads : maxThreads;

        ExecutorService executorPool = Executors.newFixedThreadPool(runningThreads);
        for (int i = strictPrefix ? 0 : -1; i < prefixList.size(); i++) {
            int finalI = i;
            FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap(strictPrefix ? finalI + 1 : finalI + 2);
            fileMap.initWriter(resultFileDir, "list");
            IOssFileProcess processor = iOssFileProcessor != null ? iOssFileProcessor.clone() : null;
            executorPool.execute(() -> {
                String endFilePrefix = "";
                String prefix = "";
                if (finalI < prefixList.size() -1 && finalI > -1) {
                    prefix = prefixList.get(finalI);
                } else {
                    if (finalI == -1) endFilePrefix = prefixList.get(0);
                    if (strictPrefix) prefix = customPrefix;
                }
                String marker = finalI == -1 ? "null" : delimiterMap.get(prefixList.get(finalI));
                ListBucket listBucket = new ListBucket(auth, configuration);
                loopListByMarker(listBucket, unitLen, prefix, endFilePrefix, marker, version, fileMap, processor, processBatch);
                listBucket.closeBucketManager();
                if (processor != null) processor.closeResource();
                fileMap.closeWriter();
            });
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