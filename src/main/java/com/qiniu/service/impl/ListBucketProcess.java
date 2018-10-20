package com.qiniu.service.impl;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.qiniu.common.*;
import com.qiniu.http.Response;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.service.oss.ListBucket;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.JsonConvertUtils;
import com.qiniu.util.ListFileFilterUtils;
import com.qiniu.util.StringUtils;
import com.qiniu.util.UrlSafeBase64;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ListBucketProcess {

    private QiniuAuth auth;
    private Configuration configuration;
    private String bucket;
    private String resultFileDir;
    private ListFileFilter listFileFilter;
    private ListFileAntiFilter listFileAntiFilter;

    public ListBucketProcess(QiniuAuth auth, Configuration configuration, String bucket, String resultFileDir) {
        this.auth = auth;
        this.configuration = configuration;
        this.bucket = bucket;
        this.resultFileDir = resultFileDir;
    }

    public void setFilter(ListFileFilter listFileFilter, ListFileAntiFilter listFileAntiFilter) {
        this.listFileFilter = listFileFilter;
        this.listFileAntiFilter = listFileAntiFilter;
    }

    private List<FileInfo> filterFileInfo(List<FileInfo> fileInfoList) {

        boolean checkListFileFilter = ListFileFilterUtils.checkListFileFilter(listFileFilter);
        boolean checkListFileAntiFilter = ListFileFilterUtils.checkListFileAntiFilter(listFileAntiFilter);

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

    private void writeAndProcess(List<FileInfo> fileInfoList, boolean filter, String endFileKey, FileReaderAndWriterMap fileMap,
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
                        .map(JsonConvertUtils::toJson)
                        .collect(Collectors.toList()))
        );

        if (filter) {
            fileInfoList = filterFileInfo(fileInfoList);
            if (fileInfoList == null || fileInfoList.size() == 0) return;
            // 如果有过滤条件的情况下，将过滤之后的结果单独写入到 other 文件中。
            if (fileMap != null) fileMap.writeOther(String.join("\n", fileInfoList.parallelStream()
                    .map(JsonConvertUtils::toJson)
                    .collect(Collectors.toList()))
            );
        }

        if (iOssFileProcessor != null) {
            if (processBatch) {
                iOssFileProcessor.processFile(fileInfoList.parallelStream()
                        .map(fileInfo -> fileInfo.key)
                        .collect(Collectors.toList()), retryCount);
            } else {
                fileInfoList.parallelStream().forEach(fileInfo -> {
                    iOssFileProcessor.processFile(fileInfo.key, retryCount);
                });
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

    public FileInfo getFirstFileInfo(Response response, String line, int version) {

        FileInfo fileInfo = new FileInfo();
        try {
            if (version == 1) {
                if (response != null) {
                    FileListing fileListing = response.jsonToObject(FileListing.class);
                    fileInfo = fileListing.items != null && fileListing.items.length > 0 ? fileListing.items[0] : null;
                }
            } else if (version == 2) {
                if (response != null || !StringUtils.isNullOrEmpty(line)) {
                    if (response != null) {
                        List<String> lineList = Arrays.asList(response.bodyString().split("\n"));
                        line = lineList.size() > 0 ? lineList.get(0) : null;
                    }
                    if (!StringUtils.isNullOrEmpty(line)) {
                        JsonObject json = JsonConvertUtils.toJsonObject(line);
                        JsonElement item = json.get("item");
                        if (item != null && !(item instanceof JsonNull)) {
                            fileInfo = JsonConvertUtils.fromJson(item, FileInfo.class);
                        }
                    }
                }
            }
        } catch (QiniuException e) {}

        return fileInfo;
    }

    private List<FileInfo> listByPrefix(ListBucket listBucket, List<String> prefixList, int version, FileReaderAndWriterMap fileMap,
                                        Queue<QiniuException> exceptionQueue, int retryCount) throws QiniuException {

        List<FileInfo> fileInfoList = prefixList.parallelStream()
                .filter(prefix -> !prefix.contains("|"))
                .map(prefix -> {
                    Response response = null;
                    FileInfo firstFileInfo = null;
                    try {
                        response = listBucket.run(bucket, prefix, null, null, 1, retryCount, version);
                        firstFileInfo = getFirstFileInfo(response, null, version);
                    } catch (QiniuException e) {
                        fileMap.writeErrorOrNull(bucket + "\t" + prefix + "\t" + e.error());
                        if (e.code() > 400) exceptionQueue.add(e);
                    } finally { if (response != null) response.close(); }
                    return firstFileInfo;
                })
                .filter(fileInfo -> !(fileInfo == null || StringUtils.isNullOrEmpty(fileInfo.key)))
                .collect(Collectors.toList());

        return fileInfoList;
    }

    private List<String> getSecondFilePrefix(List<String> prefixList, List<FileInfo> delimitedFileInfo) {
        List<String> firstKeyList = delimitedFileInfo.parallelStream()
                .map(fileInfo -> fileInfo.key)
                .collect(Collectors.toList());
        List<String> secondPrefixList = new ArrayList<>();
        for (String firstKey : firstKeyList) {
            String firstPrefix = firstKey.substring(0, 1);
            for (String secondPrefix : prefixList) {
                secondPrefixList.add(firstPrefix + secondPrefix);
            }
        }

        return secondPrefixList;
    }

    public Map<String, String> getDelimitedFileMap(boolean filter, int version, int level, String customPrefix,
                                                   String resultPrefix, IOssFileProcess iOssFileProcessor, int retryCount)
            throws IOException {

        Queue<QiniuException> exceptionQueue = new ConcurrentLinkedQueue<>();
        FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap();
        fileMap.initWriter(resultFileDir, resultPrefix);
        ListBucket listBucket = new ListBucket(auth, configuration);
        List<FileInfo> fileInfoList;
        List<String> prefixList = Arrays.asList(" !\"#$%&'()*+,-./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~".split(""));
        if (!StringUtils.isNullOrEmpty(customPrefix)) {
            prefixList = prefixList
                    .parallelStream()
                    .map(prefix -> customPrefix + prefix)
                    .collect(Collectors.toList());
        }

        fileInfoList = listByPrefix(listBucket, prefixList, version, fileMap, exceptionQueue, retryCount);
        if (level == 2) {
            prefixList = getSecondFilePrefix(prefixList, fileInfoList);
            fileInfoList = listByPrefix(listBucket, prefixList, version, fileMap, exceptionQueue, retryCount);
        }
        listBucket.closeBucketManager();
        QiniuException qiniuException = exceptionQueue.poll();
        if (qiniuException != null) throw qiniuException;
        writeAndProcess(fileInfoList, filter, "", fileMap, iOssFileProcessor, false, retryCount, exceptionQueue);
        fileMap.closeWriter();

        return fileInfoList.parallelStream().collect(Collectors.toMap(
                fileInfo -> fileInfo.key,
                fileInfo -> UrlSafeBase64.encodeToString("{\"c\":" + fileInfo.type + ",\"k\":\"" + fileInfo.key + "\"}"),
                (value1, value2) -> value2
        ));
    }

    /*
        单次列举请求，可以传递 marker 和 limit 参数，可采用此方法进行并发处理。v1 list 接口直接返回一个全部 limit（上限 1000）范围内的数据，
        v2 的 list 接口返回的是文本的数据流，可通过 java8 的流来处理。
     */
    public List<FileInfo> list(ListBucket listBucket, String bucket, String prefix, String delimiter, String marker,
                         int limit, int version, int retryCount) throws IOException {

        List<FileInfo> fileInfoList = new ArrayList<>();
        Response response = listBucket.run(bucket, prefix, delimiter, marker, limit, retryCount, version);
        if (version == 1) {
            FileListing fileListing = response.jsonToObject(FileListing.class);
            fileInfoList = Arrays.asList(fileListing.items);
        } else if (version == 2) {
            InputStream inputStream = new BufferedInputStream(response.bodyStream());
            Reader reader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(reader);
            fileInfoList = bufferedReader.lines().parallel()
                    .map(line -> getFirstFileInfo(null, line, 2))
                    .collect(Collectors.toList());
            bufferedReader.close();
            reader.close();
            inputStream.close();
        }
        response.close();
        return fileInfoList;
    }

    public String getNextMarker(List<FileInfo> fileInfoList, String fileFlag, int unitLen, String marker) {

        if (fileInfoList == null || fileInfoList.size() < unitLen) {
            return marker;
        } else if (!StringUtils.isNullOrEmpty(fileFlag) && fileInfoList.parallelStream()
                .anyMatch(fileInfo -> fileInfo.key.equals(fileFlag))) {
            return null;
        } else {
            Optional<FileInfo> lastFileInfo = fileInfoList.parallelStream().max(Comparator.comparing(fileInfo -> fileInfo.key));
            return lastFileInfo.isPresent() ? UrlSafeBase64.encodeToString("{\"c\":" + lastFileInfo.get().type +
                    ",\"k\":\"" + lastFileInfo.get().key + "\"}") : null;
        }
    }

    public void listAndProcess(ListBucket listBucket, boolean filter, int unitLen, String prefix,
                                 String endFileKey, String marker, int version, FileReaderAndWriterMap fileMap,
                                 IOssFileProcess processor, boolean processBatch) {

        while (!StringUtils.isNullOrEmpty(marker)) {
            try {
                List<FileInfo> fileInfoList = list(listBucket, bucket, prefix, "", marker.equals("null") ? "" : marker,
                        unitLen, version, 3);
                writeAndProcess(fileInfoList, filter, endFileKey, fileMap, processor, processBatch, 3, null);
                marker = getNextMarker(fileInfoList, endFileKey, unitLen, marker);
            } catch (IOException e) {
                fileMap.writeErrorOrNull(bucket + "\t" + prefix + endFileKey + "\t" + marker + "\t" + unitLen
                        + "\t" + e.getMessage());
            }
        }
    }

    public void processBucket(boolean filter, int version, int maxThreads, int level, int unitLen, boolean endFile, String customPrefix,
                              IOssFileProcess iOssFileProcessor, boolean processBatch) throws IOException, CloneNotSupportedException {

        Map<String, String> delimitedFileMap = getDelimitedFileMap(filter, version, level, customPrefix, "list", iOssFileProcessor, 3);
        List<String> keyList = new ArrayList<>(delimitedFileMap.keySet());
        Collections.sort(keyList);
        boolean strictPrefix = !StringUtils.isNullOrEmpty(customPrefix);
        int runningThreads = strictPrefix ? delimitedFileMap.size() : delimitedFileMap.size() + 1;
        runningThreads = runningThreads < maxThreads ? runningThreads : maxThreads;
        System.out.println("there are " + runningThreads + " threads running...");

        ExecutorService executorPool = Executors.newFixedThreadPool(runningThreads);
        for (int i = strictPrefix ? 0 : -1; i < keyList.size(); i++) {
            int finalI = i;
            FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap();
            fileMap.initWriter(resultFileDir, "list");
            IOssFileProcess processor = iOssFileProcessor != null ? iOssFileProcessor.clone() : null;
            executorPool.execute(() -> {
                String endFileKey = "";
                String prefix = "";
                if (endFile && finalI < keyList.size() - 1) {
                    endFileKey = keyList.get(finalI + 1);
                } else if (!endFile && finalI < keyList.size() -1 && finalI > -1) {
                    if (keyList.get(finalI).length() < customPrefix.length() + 2) prefix = keyList.get(finalI);
                    else prefix = keyList.get(finalI).substring(0, customPrefix.length() + level == 2 ? 2 : 1);
                } else {
                    if (finalI == -1) endFileKey = keyList.get(0);
                    if (strictPrefix) prefix = customPrefix;
                }
                String marker = finalI == -1 ? "null" : delimitedFileMap.get(keyList.get(finalI));
                ListBucket listBucket = new ListBucket(auth, configuration);
                listAndProcess(listBucket, filter, unitLen, prefix, endFileKey, marker, version, fileMap, processor, processBatch);
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