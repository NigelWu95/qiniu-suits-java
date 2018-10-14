package com.qiniu.service.impl;

import com.google.gson.*;
import com.qiniu.common.*;
import com.qiniu.http.Response;
import com.qiniu.interfaces.IBucketProcess;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.service.oss.ListBucket;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.JSONConvertUtils;
import com.qiniu.util.StringUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ListBucketProcess implements IBucketProcess {

    private QiniuAuth auth;
    private Configuration configuration;
    private String bucket;
    private String resultFileDir;
    private FileReaderAndWriterMap fileReaderAndWriterMap = new FileReaderAndWriterMap();

    public ListBucketProcess(QiniuAuth auth, Configuration configuration, String bucket, String resultFileDir)
            throws IOException {

        this.auth = auth;
        this.configuration = configuration;
        this.bucket = bucket;
        this.resultFileDir = resultFileDir;
        fileReaderAndWriterMap.initWriter(resultFileDir, "list");
    }

    public String[] getFirstFileInfoAndMarker(Response response, String line, FileInfo fileInfo, int version) {

        // 0-fileKey, 1-fileInfo, 2-nextMarker
        String[] firstFileInfoAndMarker = new String[]{"", "", ""};
        try {
            if (version == 1) {
                FileListing fileListing = response != null ? response.jsonToObject(FileListing.class) : null;
                if (fileListing == null && fileInfo == null) return firstFileInfoAndMarker;
                else if (fileListing != null) {
                    FileInfo[] items = fileListing.items;
                    if (items.length > 0) {
                        firstFileInfoAndMarker[0] = items[0].key;
                        firstFileInfoAndMarker[1] = JSONConvertUtils.toJson(items[0]);
                    }
                    firstFileInfoAndMarker[2] = fileListing.marker == null ? "" : fileListing.marker;
                } else {
                    firstFileInfoAndMarker[0] = fileInfo.key;
                    firstFileInfoAndMarker[1] = JSONConvertUtils.toJson(fileInfo);
                }
            } else if (version == 2) {
                if (response != null) line = response.bodyString();
                if (StringUtils.isNullOrEmpty(line)) return firstFileInfoAndMarker;
                JsonObject json = JSONConvertUtils.toJson(line);
                JsonElement item = json.get("item");
                if (item != null && !(item instanceof JsonNull)) {
                    firstFileInfoAndMarker[0] = item.getAsJsonObject().get("key").getAsString();
                    firstFileInfoAndMarker[1] = JSONConvertUtils.toJson(item);
                }
                firstFileInfoAndMarker[2] = json.get("marker").getAsString();
            }
        } catch (QiniuException e) {}

        return firstFileInfoAndMarker;
    }

    private Map<String, String> listByPrefix(ListBucket listBucket, List<String> prefixList, int version, boolean doWrite,
                                             IOssFileProcess iOssFileProcessor) throws QiniuException {

        Queue<QiniuException> exceptionQueue = new ConcurrentLinkedQueue<>();
        Map<String, String[]> fileInfoAndMarkerMap = prefixList.parallelStream()
                .filter(prefix -> !prefix.contains("|"))
                .map(prefix -> {
                        Response response = null;
                        String[] firstFileInfoAndMarker = null;
                        try {
                            response = listBucket.run(bucket, prefix, null, null, 1, 3, version);
                            firstFileInfoAndMarker = getFirstFileInfoAndMarker(response, null, null, version);
                        } catch (QiniuException e) {
                            fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + prefix + "\t" + e.error());
                            if (e.code() > 400) exceptionQueue.add(e);
                        } finally { if (response != null) response.close(); }
                        return firstFileInfoAndMarker;
        }).filter(fileInfoAndMarker -> !(fileInfoAndMarker == null || StringUtils.isNullOrEmpty(fileInfoAndMarker[0])))
            .collect(Collectors.toMap(
                    fileInfoAndMarker -> fileInfoAndMarker[1],
                    fileInfoAndMarker -> new String[]{fileInfoAndMarker[0], fileInfoAndMarker[2]},
                    (oldValue, newValue) -> newValue
        ));

        QiniuException qiniuException = exceptionQueue.poll();
        if (qiniuException == null) {
            qiniuException = processFileInfo(fileInfoAndMarkerMap.keySet(), null, doWrite ? fileReaderAndWriterMap : null,
                iOssFileProcessor, false, 3, exceptionQueue).poll();
            if (qiniuException != null) throw qiniuException;
        }
        else throw qiniuException;

        return fileInfoAndMarkerMap.values().parallelStream()
                .collect(Collectors.toMap(keyAndMarker -> keyAndMarker[0], keyAndMarker -> keyAndMarker[1]));
    }

    private List<String> getSecondFilePrefix(List<String> prefixList, Map<String, String> delimitedFileMap) {
        List<String> firstKeyList = new ArrayList<>(delimitedFileMap.keySet());
        List<String> secondPrefixList = new ArrayList<>();
        for (String firstKey : firstKeyList) {
            String firstPrefix = firstKey.substring(0, 1);
            if (StringUtils.isNullOrEmpty(delimitedFileMap.get(firstKey))) {
                secondPrefixList.add(firstPrefix);
                continue;
            }
            for (String secondPrefix : prefixList) {
                secondPrefixList.add(firstPrefix + secondPrefix);
            }
        }

        return secondPrefixList;
    }

    public Map<String, String> getDelimitedFileMap(int version, int level, IOssFileProcess iOssFileProcessor) throws QiniuException {
        List<String> prefixList = Arrays.asList(" !\"#$%&'()*+,-./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~".split(""));
        Map<String, String> delimitedFileMap;
        ListBucket listBucket = new ListBucket(auth, configuration);

        if (level == 2) {
            delimitedFileMap = listByPrefix(listBucket, prefixList, version, false, null);
            prefixList = getSecondFilePrefix(prefixList, delimitedFileMap);
            delimitedFileMap.putAll(listByPrefix(listBucket, prefixList, version, true, iOssFileProcessor));
        } else {
            delimitedFileMap = listByPrefix(listBucket, prefixList, version, true, iOssFileProcessor);
        }
        listBucket.closeBucketManager();
        fileReaderAndWriterMap.closeWriter();

        return delimitedFileMap;
    }

    /*
    单次列举请求，可以传递 marker 和 limit 参数，通常采用此方法进行并发处理
     */
    public FileListing listV1(ListBucket listBucket, String bucket, String prefix, String delimiter, String marker,
                              int limit, FileReaderAndWriterMap fileReaderAndWriterMap, int retryCount) {
        FileListing fileListing = null;
        try {
            Response response = listBucket.run(bucket, prefix, delimiter, marker, limit, retryCount, 1);
            fileListing = response.jsonToObject(FileListing.class);
            response.close();
        } catch (Exception e) {
            fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + prefix + "\t" + delimiter + "\t" + marker + "\t" + limit + "\t" + e.getMessage());
        }

        return fileListing;
    }

    public Map<String, String> getFileInfoAndMarkerMap(FileListing fileListing) {

        if (fileListing == null) return new HashMap<>();
        return Arrays.asList(fileListing.items).parallelStream()
                .map(item -> getFirstFileInfoAndMarker(null, null, item, 1))
                .collect(Collectors.toMap(
                        fileInfoAndMarker -> fileInfoAndMarker[1],
                        fileInfoAndMarker -> fileInfoAndMarker[2]
        ));
    }

    /*
    v2 的 list 接口，接收到响应后通过 java8 的流来处理响应的文本流。
     */
    public Map<String, String> listV2(ListBucket listBucket, String bucket, String prefix, String delimiter, String marker,
                         int limit, FileReaderAndWriterMap fileReaderAndWriterMap, int retryCount) {
        Map<String, String> fileInfoAndMarkerMap = new HashMap<>();
        try {
            Response response = listBucket.run(bucket, prefix, delimiter, marker, limit, retryCount, 2);
            InputStream inputStream = new BufferedInputStream(response.bodyStream());
            Reader reader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(reader);
            Stream<String> lineStream = bufferedReader.lines().parallel();
            fileInfoAndMarkerMap = lineStream.
                    map(line -> getFirstFileInfoAndMarker(null, line, null, 2))
                    .collect(Collectors.toMap(
                            fileInfoAndMarker -> fileInfoAndMarker[1],
                            fileInfoAndMarker -> fileInfoAndMarker[2]
            ));
            bufferedReader.close();
            reader.close();
            inputStream.close();
            response.close();
        } catch (Exception e) {
            fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + prefix + "\t" + marker + "\t" + limit + "\t" + e.getMessage());
        }

        return fileInfoAndMarkerMap;
    }

    public String getNextMarker(Map<String, String> fileInfoAndMarkerMap, String fileFlag, boolean endFile) {

        if (fileInfoAndMarkerMap == null) return null;
        if (endFile && fileInfoAndMarkerMap.keySet().parallelStream()
                .anyMatch(fileInfo -> JSONConvertUtils.fromJson(fileInfo, FileInfo.class).key.equals(fileFlag)))
        {
            return null;
        } else {
            Optional<String> lastFileInfo = fileInfoAndMarkerMap.keySet().parallelStream().max(String::compareTo);
            return lastFileInfo.isPresent() ? fileInfoAndMarkerMap.get(lastFileInfo.get()) : null;
        }
    }

    public Queue<QiniuException> processFileInfo(Set<String> fileInfoList, String fileFlag, FileReaderAndWriterMap fileMap,
                                IOssFileProcess iOssFileProcessor, boolean processBatch, int retryCount,
                                Queue<QiniuException> exceptionQueue) {

        if (fileInfoList == null) return exceptionQueue;
        Stream<String> fileInfoStream = fileInfoList.parallelStream();
        if (!StringUtils.isNullOrEmpty(fileFlag)) {
            fileInfoStream = fileInfoStream.filter(fileInfo -> JSONConvertUtils.fromJson(fileInfo, FileInfo.class).key.compareTo(fileFlag) < 0);
        }

        if (iOssFileProcessor == null) {
            if (fileMap != null) fileMap.writeSuccess(String.join("\n", fileInfoStream.collect(Collectors.toList())));
            return exceptionQueue;
        }

        if (fileMap != null && exceptionQueue != null) {
            fileInfoStream.forEach(fileInfo -> {
                fileMap.writeSuccess(fileInfo);
                iOssFileProcessor.processFile(fileInfo, retryCount, processBatch);
                if (iOssFileProcessor.qiniuException() != null && iOssFileProcessor.qiniuException().code() > 400)
                    exceptionQueue.add(iOssFileProcessor.qiniuException());
            });
        } else if (exceptionQueue != null) {
            fileInfoStream.forEach(fileInfo -> {
                iOssFileProcessor.processFile(fileInfo, retryCount, processBatch);
                if (iOssFileProcessor.qiniuException() != null && iOssFileProcessor.qiniuException().code() > 400)
                    exceptionQueue.add(iOssFileProcessor.qiniuException());
            });
        } else if (fileMap != null) {
            fileInfoStream.forEach(fileInfo -> {
                fileMap.writeSuccess(fileInfo);
                iOssFileProcessor.processFile(fileInfo, retryCount, processBatch);
            });
        } else {
            fileInfoStream.forEach(fileInfo -> iOssFileProcessor.processFile(fileInfo, retryCount, processBatch));
        }

        return exceptionQueue;
    }

    public void processBucket(IOssFileProcess iOssFileProcessor, boolean processBatch, int version, int maxThreads,
                              int level, int unitLen, boolean endFile) throws IOException, CloneNotSupportedException {

        Map<String, String> delimitedFileMap = getDelimitedFileMap(version, level, iOssFileProcessor);
        List<String> keyList = new ArrayList<>(delimitedFileMap.keySet());
        Collections.sort(keyList);
        int runningThreads = delimitedFileMap.size() < maxThreads ? delimitedFileMap.size() : maxThreads;
        System.out.println("there are " + runningThreads + " threads running...");

        ExecutorService executorPool = Executors.newFixedThreadPool(runningThreads);
        for (int i = 0; i < keyList.size(); i++) {
            int finalI = i;
            FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap();
            fileMap.initWriter(resultFileDir, "list");
            IOssFileProcess processor = iOssFileProcessor != null ? iOssFileProcessor.clone() : null;
            executorPool.execute(() -> {
                String endFileKey = finalI == keyList.size() - 1 ? "" : keyList.get(finalI + 1);
                String prefix = endFile ? null :
                        level == 2 ? keyList.get(finalI).substring(0,2) : keyList.get(finalI).substring(0, 1);
                String marker = delimitedFileMap.get(keyList.get(finalI));
                ListBucket listBucket = new ListBucket(auth, configuration);
                Map<String, String> fileInfoAndMarkerMap = new HashMap<>();
                while (!StringUtils.isNullOrEmpty(marker)) {
                    if (version == 2) {
                        fileInfoAndMarkerMap = listV2(listBucket, bucket, prefix, "", marker, unitLen, fileMap, 3);
                        marker = getNextMarker(fileInfoAndMarkerMap, endFileKey, endFile);
                    } else if (version == 1) {
                        FileListing fileListing = listV1(listBucket, bucket, prefix, "", marker, unitLen, fileMap, 3);
                        fileInfoAndMarkerMap = getFileInfoAndMarkerMap(fileListing);
                        marker = getNextMarker(fileInfoAndMarkerMap, endFileKey, endFile) != null ? fileListing.marker : null;
                    }
                    processFileInfo(fileInfoAndMarkerMap.keySet(), endFile ? endFileKey : null, fileMap, iOssFileProcessor,
                            processBatch, 3, null);
                }
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