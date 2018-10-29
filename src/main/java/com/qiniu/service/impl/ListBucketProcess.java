package com.qiniu.service.impl;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.qiniu.common.*;
import com.qiniu.http.Response;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.model.ListResult;
import com.qiniu.model.ListV2Line;
import com.qiniu.service.oss.ListBucket;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class ListBucketProcess {

    private Auth auth;
    private Configuration configuration;
    private String bucket;
    private int unitLen;
    private int version;
    private String resultFileDir;
    private String customPrefix;
    private List<String> antiPrefix;
    private int retryCount;
    private ListFileFilter listFileFilter;
    private ListFileAntiFilter listFileAntiFilter;
    private boolean checkListFileFilter;
    private boolean checkListFileAntiFilter;
    private List<String> originPrefixList = Arrays.asList(
            " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
            .split(""));

    public ListBucketProcess(Auth auth, Configuration configuration, String bucket, int unitLen, int version,
                             String resultFileDir, String customPrefix, List<String> antiPrefix, int retryCount) {
        this.auth = auth;
        this.configuration = configuration;
        this.bucket = bucket;
        this.unitLen = unitLen;
        this.version = version;
        this.resultFileDir = resultFileDir;
        this.customPrefix = customPrefix;
        this.antiPrefix = antiPrefix;
        this.retryCount = retryCount;
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

    private void writeAndProcess(List<FileInfo> fileInfoList, String endFile, FileReaderAndWriterMap fileReaderAndWriterMap,
                                 IOssFileProcess iOssFileProcessor, boolean processBatch) throws QiniuException {

        if (fileInfoList == null || fileInfoList.size() == 0) return;
        fileInfoList = fileInfoList.parallelStream()
                .filter(fileInfo ->  StringUtils.isNullOrEmpty(endFile) ? fileInfo != null :
                        fileInfo != null && endFile.compareTo(fileInfo.key) > 0)
                .collect(Collectors.toList());
        if (fileInfoList == null || fileInfoList.size() == 0) return;
        // 如果 list 不为空，将完整的列表先写入。
        if (fileReaderAndWriterMap != null) fileReaderAndWriterMap.writeSuccess(
                String.join("\n", fileInfoList.parallelStream()
                        .map(JsonConvertUtils::toJsonWithoutUrlEscape)
                        .collect(Collectors.toList()))
        );

        if (checkListFileFilter || checkListFileAntiFilter) {
            fileInfoList = filterFileInfo(fileInfoList);
            if (fileInfoList == null || fileInfoList.size() == 0) return;
            // 如果有过滤条件的情况下，将过滤之后的结果单独写入到 other 文件中。
            if (fileReaderAndWriterMap != null) fileReaderAndWriterMap.writeOther(String.join("\n",
                    fileInfoList.parallelStream().filter(Objects::nonNull)
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

            if (iOssFileProcessor.qiniuException() != null && iOssFileProcessor.qiniuException().code() > 400)
                throw iOssFileProcessor.qiniuException();
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
                List<ListV2Line> listV2LineList = bufferedReader.lines().parallel()
                        .filter(line -> !StringUtils.isNullOrEmpty(line))
                        .map(this::getItemByList2Line)
                        .collect(Collectors.toList());
                Optional<ListV2Line> lastListV2Line = listV2LineList.parallelStream()
                        .max(ListV2Line::compareTo);
                listResult.fileInfoList = listV2LineList.parallelStream()
                        .map(listV2Line -> listV2Line.fileInfo)
                        .collect(Collectors.toList());
                listResult.nextMarker = lastListV2Line.map(listV2Line -> listV2Line.marker).orElse(null);
            }
        }

        return listResult;
    }

    private List<ListResult> preListByPrefix(ListBucket listBucket, List<String> prefixList, int unitLen, String resultPrefix)
            throws IOException {
        FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap();
        fileMap.initWriter(resultFileDir, resultPrefix, "pre");
        List<ListResult> listResultList = prefixList.parallelStream()
                .map(prefix -> {
                    Response response = null;
                    ListResult listResult = new ListResult();
                    try {
                        response = listBucket.run(bucket, prefix, null, null, unitLen, retryCount, version);
                        listResult = getListResult(response, version);
                        listResult.commonPrefix = prefix;
                    } catch (QiniuException e) {
                        fileMap.writeErrorOrNull(prefix + "\t" + e.error());
                    } finally {
                        if (response != null) response.close();
                    }
                    return listResult;
                })
                .filter(ListResult::isValid)
                .collect(Collectors.toList());
        fileMap.closeWriter();
        return listResultList;
    }

    public List<ListResult> preList(int unitLen, int level, String customPrefix, List<String> antiPrefix, String resultPrefix)
            throws IOException {
        List<String> validPrefixList = originPrefixList.parallelStream()
                .filter(originPrefix -> !antiPrefix.contains(originPrefix))
                .map(prefix -> StringUtils.isNullOrEmpty(customPrefix) ? prefix : customPrefix + prefix)
                .collect(Collectors.toList());
        List<ListResult> listResultList = new ArrayList<>();
        ListBucket preListBucket = new ListBucket(auth, configuration);
        if (level == 1) {
            listResultList = preListByPrefix(preListBucket, validPrefixList, unitLen, resultPrefix);
        } else if (level == 2) {
            listResultList = preListByPrefix(preListBucket, validPrefixList, 1, resultPrefix);
            List<String> level2PrefixList = listResultList.parallelStream()
                    .map(singlePrefixListResult -> originPrefixList.parallelStream()
                            .filter(originPrefix -> !antiPrefix.contains(originPrefix))
                            .map(originPrefix -> singlePrefixListResult.commonPrefix + originPrefix)
                            .collect(Collectors.toList())
                    )
                    .reduce((list1, list2) -> {
                        list1.addAll(list2);
                        return list1;
                    })
                    .get();
            listResultList = preListByPrefix(preListBucket, level2PrefixList, unitLen, resultPrefix);
        }

        return listResultList;
    }

    public void checkValidPrefix(int level, String customPrefix, List<String> antiPrefix)
            throws IOException {
        List<ListResult> listResultList = preList(1, level, customPrefix, antiPrefix, "check");
        FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap();
        fileMap.initWriter(resultFileDir, "list", "check");
        List<String> validPrefixAndMarker = listResultList.parallelStream()
                .map(listResult -> listResult.commonPrefix + "\t" + listResult.nextMarker)
                .collect(Collectors.toList());
        fileMap.writeSuccess(String.join("\n", validPrefixAndMarker));
        fileMap.closeWriter();
    }

    private void recordProgress(String prefix, String endFile, String marker, FileReaderAndWriterMap fileMap) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("prefix", prefix);
        jsonObject.addProperty("end", endFile);
        jsonObject.addProperty("marker", marker);
        fileMap.writeKeyFile("marker" + fileMap.getSuffix(), JsonConvertUtils.toJsonWithoutUrlEscape(jsonObject));
        jsonObject = null;
    }

    private void loopList(ListBucket listBucket, String prefix, String endFile, String marker, FileReaderAndWriterMap fileMap,
                          IOssFileProcess processor, boolean processBatch) {
        recordProgress(prefix, endFile, marker, fileMap);
        while (!StringUtils.isNullOrEmpty(marker)) {
            try {
                Response response = listBucket.run(bucket, prefix, "", "null".equals(marker) ? "" : marker, unitLen,
                        retryCount, version);
                ListResult listResult = getListResult(response, version);
                response.close();
                List<FileInfo> fileInfoList = listResult.fileInfoList;
                writeAndProcess(fileInfoList, endFile, fileMap, processor, processBatch);
                marker = !StringUtils.isNullOrEmpty(endFile) && fileInfoList.parallelStream()
                        .anyMatch(fileInfo -> fileInfo != null && endFile.compareTo(fileInfo.key) < 0) ?
                        "" : listResult.nextMarker;
                recordProgress(prefix, endFile, marker, fileMap);
            } catch (Exception e) {
                fileMap.writeErrorOrNull(prefix + endFile + "\t" + marker + "\t" + e.getMessage());
            }
        }
    }

    public void straightList(String prefix, String endFile, String marker, IOssFileProcess iOssFileProcessor, boolean processBatch)
            throws IOException {
        FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap();
        fileMap.initWriter(resultFileDir, "list", "total");
        ListBucket listBucket = new ListBucket(auth, configuration);
        marker = StringUtils.isNullOrEmpty(marker) ? "null" : marker;
        loopList(listBucket, prefix, endFile, marker, fileMap, iOssFileProcessor, processBatch);
        fileMap.closeWriter();
    }

    private void listTotalWithPrefix(ExecutorService executorPool, List<ListResult> listResultList, IOssFileProcess iOssFileProcessor,
                                    boolean processBatch) throws IOException, CloneNotSupportedException {

        listResultList.sort(Comparator.comparing(listResult -> listResult.commonPrefix));
        for (int i = StringUtils.isNullOrEmpty(customPrefix) ? -1 : 0; i < listResultList.size(); i++) {
            int finalI = i;
            int resultIndex = StringUtils.isNullOrEmpty(customPrefix) ? i + 2 : i + 1;
            FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap();
            fileMap.initWriter(resultFileDir, "list", resultIndex);
            IOssFileProcess processor = iOssFileProcessor != null ? iOssFileProcessor.getNewInstance(resultIndex) : null;
            writeAndProcess(i > -1 ? listResultList.get(i).fileInfoList : null, "", fileMap, processor, processBatch);
            executorPool.execute(() -> {
                String endFilePrefix = "";
                String prefix = "";
                String marker = "null";
                if (finalI < listResultList.size() -1 && finalI > -1) {
                    prefix = listResultList.get(finalI).commonPrefix;
                    marker = listResultList.get(finalI).nextMarker;
                } else {
                    if (finalI == -1) endFilePrefix = listResultList.get(0).commonPrefix;
                    else {
                        marker = listResultList.get(finalI).nextMarker;
                        if (StringUtils.isNullOrEmpty(marker)) {
                            FileInfo fileInfo = listResultList.get(finalI).fileInfoList.parallelStream()
                                    .max(Comparator.comparing(fileInfo1 -> fileInfo1.key))
                                    .get();
                            JsonObject jsonObject = new JsonObject();
                            jsonObject.addProperty("c", fileInfo.type);
                            jsonObject.addProperty("k", fileInfo.key);
                            marker = UrlSafeBase64.encodeToString(JsonConvertUtils.toJson(jsonObject));
                        }
                    }
                    if (!StringUtils.isNullOrEmpty(customPrefix)) prefix = customPrefix;
                }
                ListBucket listBucket = new ListBucket(auth, configuration);
                loopList(listBucket, prefix, endFilePrefix, marker, fileMap, processor, processBatch);
                if (processor != null) processor.closeResource();
                fileMap.closeWriter();
            });
        }
    }

    private ExecutorService getActualExecutorPool(int listSize, int maxThreads) {
        int runningThreads = StringUtils.isNullOrEmpty(customPrefix) ? listSize + 1 : listSize;
        runningThreads = runningThreads < maxThreads ? runningThreads : maxThreads;
        System.out.println("list bucket concurrently running with " + runningThreads + " threads ...");
        return Executors.newFixedThreadPool(runningThreads);
    }

    public void processBucket(int maxThreads, int level, IOssFileProcess iOssFileProcessor, boolean processBatch)
            throws IOException, CloneNotSupportedException {

        List<ListResult> listResultList = preList(unitLen, level, customPrefix, antiPrefix, "list");
        ExecutorService executorPool = getActualExecutorPool(listResultList.size(), maxThreads);
        listTotalWithPrefix(executorPool, listResultList, iOssFileProcessor, processBatch);
        executorPool.shutdown();
        try {
            while (!executorPool.isTerminated())
                Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}