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

    private void writeAndProcess(List<FileInfo> fileInfoList, String endFileKey, FileReaderAndWriterMap fileMap,
                                 IOssFileProcess iOssFileProcessor, boolean processBatch, int retryCount,
                                 Queue<QiniuException> exceptionQueue) throws QiniuException {

        if (fileInfoList == null || fileInfoList.size() == 0) return;
        if (!"".equals(endFileKey)) {
            fileInfoList = fileInfoList.parallelStream()
                            .filter(fileInfo -> fileInfo.key.compareTo(endFileKey) < 0)
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

    public FileInfo decodeMarker (String marker) {
        if (StringUtils.isNullOrEmpty(marker)) return new FileInfo();
        String itemJson = new String(UrlSafeBase64.decode(marker.replace('-', '+').replace('_', '/')));
        JsonObject jsonObject = JsonConvertUtils.toJsonObject(itemJson);
        FileInfo fileInfo = new FileInfo();
        fileInfo.key = jsonObject.get("k").getAsString();
        fileInfo.type = jsonObject.get("c").getAsInt();
        return fileInfo;
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
                if (item == null || item instanceof JsonNull) {
                    listV2Line.fileInfo = new FileInfo();
                    listV2Line.fileInfo.key = decodeMarker(listV2Line.marker).key;
                }
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
                        listResult.lastFileKey = items[items.length - 1].key;
                        listResult.fileInfoList = Arrays.asList(items);
                    } else {
                        listResult.lastFileKey = decodeMarker(listResult.nextMarker).key;
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
                    listResult.lastFileKey = listV2Line.fileInfo.key;
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

    private List<ListResult> listByPrefix(ListBucket listBucket, List<String> prefixList, int unitLen, int version, FileReaderAndWriterMap fileMap,
                                        Queue<QiniuException> exceptionQueue, int retryCount) {

        return prefixList.parallelStream()
                .map(prefix -> {
                    Response response = null;
                    ListResult listResult = null;
                    try {
                        response = listBucket.run(bucket, prefix, null, null, unitLen, retryCount, version);
                        listResult = getListResult(response, version);
                    } catch (QiniuException e) {
                        fileMap.writeErrorOrNull(bucket + "\t" + prefix + "\t" + e.error());
                        if (e.code() > 400) exceptionQueue.add(e);
                    } finally { if (response != null) response.close(); }
                    return listResult;
                })
                .filter(listResult -> listResult != null && !StringUtils.isNullOrEmpty(listResult.lastFileKey))
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

    public Map<String, String> getDelimitedFileMap(int version, int unitLen, int level, String customPrefix, String resultPrefix,
                                                   IOssFileProcess iOssFileProcessor, boolean processBatch, int retryCount) throws IOException {
        Queue<QiniuException> exceptionQueue = new ConcurrentLinkedQueue<>();
        FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap();
        fileMap.initWriter(resultFileDir, resultPrefix);
        ListBucket listBucket = new ListBucket(auth, configuration);
        List<String> originPrefixList = Arrays.asList(" !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~".split(""));
        List<String> prefixList = !StringUtils.isNullOrEmpty(customPrefix) ? originPrefixList
                .parallelStream()
                .map(prefix -> customPrefix + prefix)
                .collect(Collectors.toList()) : originPrefixList;

        List<ListResult> listResultList = listByPrefix(listBucket, prefixList, unitLen, version, fileMap, exceptionQueue, retryCount);
        processDelimitedFileInfo(listResultList, fileMap, iOssFileProcessor, processBatch, retryCount, exceptionQueue);
        if (level == 2) {
            List<String> secondPrefixList = new ArrayList<>();
            List<String> delimiterFileList = listResultList.parallelStream()
                    .filter(listResult -> listResult.fileInfoList.size() > 0 && listResult.fileInfoList.size() < unitLen)
                    .map(listResult -> listResult.lastFileKey)
                    .collect(Collectors.toList());
            for (String key : delimiterFileList) {
                String firstPrefix = key.substring(0, customPrefix.length() + 1);
                for (String secondPrefix : originPrefixList) {
                    secondPrefixList.add(firstPrefix + secondPrefix);
                }
            }
            listResultList = listByPrefix(listBucket, secondPrefixList, unitLen, version, fileMap, exceptionQueue, retryCount);
        }
        listBucket.closeBucketManager();
        processDelimitedFileInfo(listResultList, fileMap, iOssFileProcessor, processBatch, retryCount, exceptionQueue);
        fileMap.closeWriter();

        return listResultList.parallelStream()
                .collect(Collectors.toMap(
                        listResult -> listResult.lastFileKey,
                        listResult -> listResult.nextMarker,
                        (listResult1, listResult2) -> listResult1
                ));
    }

    public void loopListByMarker(ListBucket listBucket, int unitLen, String prefix, String endFileKey, String marker,
                                 int version, FileReaderAndWriterMap fileMap, IOssFileProcess processor, boolean processBatch) {
        while (!StringUtils.isNullOrEmpty(marker)) {
            try {
                Response response = listBucket.run(bucket, prefix, "", marker.equals("null") ? "" : marker, unitLen, 3, version);
                ListResult listResult = getListResult(response, version);
                response.close();
                // 写入和处理时过滤掉已删除的文件（已删除的文件信息包含的 hash 值一定为空）
                List<FileInfo> fileInfoList = listResult.fileInfoList;
                writeAndProcess(fileInfoList.stream()
                                            .filter(fileInfo -> !StringUtils.isNullOrEmpty(fileInfo.hash))
                                            .collect(Collectors.toList()),
                        endFileKey, fileMap, processor, processBatch, 3, null);
                marker = (!StringUtils.isNullOrEmpty(endFileKey) && fileInfoList.parallelStream()
                        .anyMatch(fileInfo -> fileInfo.key.equals(endFileKey))) ? null : listResult.nextMarker;
            } catch (IOException e) {
                fileMap.writeErrorOrNull(bucket + "\t" + prefix + endFileKey + "\t" + marker + "\t" + unitLen
                        + "\t" + e.getMessage());
            }
        }
    }

    public void processBucket(int version, int maxThreads, int level, int unitLen, boolean endFile, String customPrefix,
                              IOssFileProcess iOssFileProcessor, boolean processBatch) throws IOException, CloneNotSupportedException {

        Map<String, String> delimitedFileMap = getDelimitedFileMap(version, unitLen, level, customPrefix, "list",
                iOssFileProcessor, processBatch, 3);
        List<String> keyList = new ArrayList<>(delimitedFileMap.keySet());
        Collections.sort(keyList);
        boolean strictPrefix = !StringUtils.isNullOrEmpty(customPrefix);
        int runningThreads = strictPrefix ? delimitedFileMap.size() : delimitedFileMap.size() + 1;
        runningThreads = runningThreads < maxThreads ? runningThreads : maxThreads;
        System.out.println("there are " + runningThreads + " threads running...");

        ExecutorService executorPool = Executors.newFixedThreadPool(runningThreads);
        for (int i = strictPrefix ? 0 : -1; i < keyList.size(); i++) {
            int finalI = i;
            FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap(strictPrefix ? finalI + 1 : finalI + 2);
            fileMap.initWriter(resultFileDir, "list");
            IOssFileProcess processor = iOssFileProcessor != null ? iOssFileProcessor.clone() : null;
            executorPool.execute(() -> {
                String endFileKey = "";
                String prefix = "";
                if (endFile && finalI < keyList.size() - 1) {
                    endFileKey = keyList.get(finalI + 1);
                } else if (!endFile && finalI < keyList.size() -1 && finalI > -1) {
                    if (keyList.get(finalI).length() < customPrefix.length() + 2) prefix = keyList.get(finalI);
                    else prefix = keyList.get(finalI).substring(0, customPrefix.length() + (level == 2 ? 2 : 1));
                } else {
                    if (finalI == -1) endFileKey = keyList.get(0);
                    if (strictPrefix) prefix = customPrefix;
                }
                String marker = finalI == -1 ? "null" : delimitedFileMap.get(keyList.get(finalI));
                ListBucket listBucket = new ListBucket(auth, configuration);
                loopListByMarker(listBucket, unitLen, prefix, endFileKey, marker, version, fileMap, processor, processBatch);
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