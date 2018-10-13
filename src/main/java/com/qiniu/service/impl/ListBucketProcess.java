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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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

    public String[] getFirstFileInfoAndMarker(Response response, String line, FileListing fileListing, int version) throws QiniuException {

        String[] firstFileInfoAndMarker = new String[]{"", "", ""};
        if (version == 1) {
            if (response != null) fileListing = response.jsonToObject(FileListing.class);
            if (fileListing == null) return firstFileInfoAndMarker;
            FileInfo[] items = fileListing.items;
            firstFileInfoAndMarker[0] = "";
            firstFileInfoAndMarker[1] = "";
            firstFileInfoAndMarker[2] = fileListing.marker;

            if (items.length > 0) {
                firstFileInfoAndMarker[0] = items[0].key;
                firstFileInfoAndMarker[1] = JSONConvertUtils.toJson(items[0]);
            }
        } else if (version == 2) {
            if (response != null) line = response.bodyString();
            if (StringUtils.isNullOrEmpty(line)) return firstFileInfoAndMarker;
            JsonObject json = JSONConvertUtils.toJson(line);
            JsonElement item = json.get("item");
            firstFileInfoAndMarker[0] = "";
            firstFileInfoAndMarker[1] = "";
            firstFileInfoAndMarker[2] = json.get("marker").getAsString();

            if (item != null && !(item instanceof JsonNull)) {
                firstFileInfoAndMarker[0] = item.getAsJsonObject().get("key").getAsString();
                firstFileInfoAndMarker[1] = JSONConvertUtils.toJson(json.getAsJsonObject("item"));
            }
        }

        return firstFileInfoAndMarker;
    }

    private Map<String, String> listByPrefixWithParallel(ListBucket listBucket, List<String> prefixList, int version, boolean doWrite,
                                             boolean doProcess, IOssFileProcess iOssFileProcessor) throws QiniuException {

        Queue<QiniuException> qiniuExceptionQueue = new ConcurrentLinkedQueue<>();
        Map<String, String[]> fileInfoAndMarkerMap = prefixList.parallelStream()
                .filter(prefix -> !prefix.contains("|"))
                .map(prefix -> {
            Response response = null;
            String[] firstFileInfoAndMarker = null;
            try {
                response = listBucket.run(bucket, prefix, null, null, 1, 3, version);
                firstFileInfoAndMarker = getFirstFileInfoAndMarker(response, null, null, version);
            } catch (QiniuException e) {
                System.out.println(e.code() + "\t" + e.error());
                fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + prefix + "\t" + e.error());
                if (e.code() > 400) qiniuExceptionQueue.add(e);
            } finally {
                if (response != null)
                    response.close();
            }
            return firstFileInfoAndMarker;
//        }).filter(Objects::nonNull)
        }).filter(fileInfoAndMarker -> !(fileInfoAndMarker == null || StringUtils.isNullOrEmpty(fileInfoAndMarker[0])))
            .collect(Collectors
                .toMap(
                    fileInfoAndMarker -> fileInfoAndMarker[1],
                    fileInfoAndMarker -> new String[]{fileInfoAndMarker[0], fileInfoAndMarker[2]},
                    (oldValue, newValue) -> newValue
        ));

        QiniuException qiniuException = qiniuExceptionQueue.poll();
        if (qiniuException != null) throw qiniuException;

        Map<String, String> delimitedFileMap = fileInfoAndMarkerMap.values()
                .parallelStream().collect(Collectors
                        .toMap(keyAndMarker -> keyAndMarker[0], keyAndMarker -> keyAndMarker[1]));
        Stream<String> fileInfoStream = fileInfoAndMarkerMap.keySet().parallelStream();
        if (doWrite && doProcess) {
            fileInfoStream.forEach(fileInfo -> {
                fileReaderAndWriterMap.writeSuccess(fileInfo);
                iOssFileProcessor.processFile(fileInfo, 3);
            });
        } else if (doWrite) {
            fileInfoStream.forEach(fileInfo -> fileReaderAndWriterMap.writeSuccess(fileInfo));
        }

        return delimitedFileMap;
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
        boolean doProcess = iOssFileProcessor != null;
        ListBucket listBucket = new ListBucket(auth, configuration);

        if (level == 2) {
            delimitedFileMap = listByPrefixWithParallel(listBucket, prefixList, version, false, doProcess, iOssFileProcessor);
            prefixList = getSecondFilePrefix(prefixList, delimitedFileMap);
            delimitedFileMap.putAll(listByPrefixWithParallel(listBucket, prefixList, version, true, doProcess, iOssFileProcessor));
        } else {
            delimitedFileMap = listByPrefixWithParallel(listBucket, prefixList, version, true, doProcess, iOssFileProcessor);
        }

        listBucket.closeBucketManager();

        return delimitedFileMap;
    }

    /*
    单次列举，可以传递 marker 和 limit 参数，通常采用此方法进行并发处理
     */
    public String doListV1(ListBucket listBucket, String bucket, String prefix, String marker, int limit, FileReaderAndWriterMap fileReaderAndWriterMap,
                           IOssFileProcess iOssFileProcessor, boolean processBatch, int retryCount) {

        FileListing fileListing;
        if (iOssFileProcessor == null) {
            fileListing = doListV1(listBucket, bucket, prefix, "", marker, limit, true, retryCount);
            return fileListing == null ? null : fileListing.marker;
        } else {
            fileListing = doListV1(listBucket, bucket, prefix, "", marker, limit, true, retryCount);
            FileInfo[] items = fileListing.items;
            String fileInfo;

            for (FileInfo item : items) {
                fileInfo = JSONConvertUtils.toJson(item);
                fileReaderAndWriterMap.writeSuccess(fileInfo);
                if (processBatch) iOssFileProcessor.batchProcessFile(fileInfo, retryCount);
                else iOssFileProcessor.processFile(fileInfo, retryCount);

            }

            return fileListing.marker;
        }

    }

    public String doListV1(ListBucket listBucket, String bucket, String marker, int limit, String endFile, FileReaderAndWriterMap fileReaderAndWriterMap,
                           IOssFileProcess iOssFileProcessor, boolean processBatch, int retryCount) {

        FileListing fileListing = doListV1(listBucket, bucket, "", "", marker, limit, false, retryCount);
        FileInfo[] items = fileListing.items;
        String fileInfo;

        for (FileInfo item : items) {
            if (item.key.equals(endFile)) return null;
            fileInfo = JSONConvertUtils.toJson(item);
            fileReaderAndWriterMap.writeSuccess(fileInfo);
            if (iOssFileProcessor != null) {
                if (processBatch) iOssFileProcessor.batchProcessFile(fileInfo, retryCount);
                else iOssFileProcessor.processFile(fileInfo, retryCount);
            }
        }

        return fileListing.marker;
    }

    public FileListing doListV1(ListBucket listBucket, String bucket, String prefix, String delimiter, String marker,
                                int limit, boolean totalWrite, int retryCount) {

        Response response = null;
        FileListing fileListing = new FileListing();

        try {
            response = listBucket.run(bucket, prefix, delimiter, marker, limit, retryCount, 1);
            String resultBody = response.bodyString();
            JsonObject jsonObject = new Gson().fromJson(resultBody, JsonObject.class);
            JsonArray jsonArray = jsonObject.getAsJsonArray("items");
            String result = jsonArray.toString().replaceAll("[\\[\\]]", "").replaceAll("\\},\\{", "\\}\n\\{");
            if (totalWrite) fileReaderAndWriterMap.writeSuccess(result);
            fileListing = JSONConvertUtils.fromJson(resultBody, FileListing.class);
        } catch (Exception e) {
            fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + prefix + "\t" + delimiter + "\t" + marker + "\t" + limit + "\t" + e.getMessage());
        } finally {
            if (response != null) response.close();
        }

        return fileListing;
    }

    /*
    v2 的 list 接口，接收到响应后通过 java8 的流来处理响应的文本流。
     */
    public String doListV2(ListBucket listBucket, String bucket, String marker, int limit, String endFile, FileReaderAndWriterMap fileReaderAndWriterMap,
                           IOssFileProcess iOssFileProcessor, boolean processBatch, boolean withParallel, int retryCount) {

        Response response = null;
        AtomicBoolean endFlag = new AtomicBoolean(false);
        AtomicReference<String> endMarker = new AtomicReference<>();

        try {
            response = listBucket.run(bucket, "", "", marker, limit, retryCount, 2);
            InputStream inputStream = new BufferedInputStream(response.bodyStream());
            Reader reader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(reader);
            Stream<String> lineStream = withParallel ? bufferedReader.lines().parallel() : bufferedReader.lines();
            lineStream.forEach(line -> {
                String[] firstFileInfoAndMarker = new String[3];
                try { firstFileInfoAndMarker = getFirstFileInfoAndMarker(null, line, null, 2);
                } catch (QiniuException e) {}
                String fileKey = firstFileInfoAndMarker[0];
                String fileInfo = firstFileInfoAndMarker[1];
                String nextMarker = firstFileInfoAndMarker[2];
                if (endFile.equals(fileKey)) {
                    endFlag.set(true);
                    endMarker.set(null);
                }
                if (!endFlag.get()) {
                    fileReaderAndWriterMap.writeSuccess(fileInfo);
                    endMarker.set(nextMarker);
                    if (iOssFileProcessor != null) {
                        if (processBatch) iOssFileProcessor.batchProcessFile(fileInfo, retryCount);
                        else iOssFileProcessor.processFile(fileInfo, retryCount);
                    }
                }
            });
            bufferedReader.close();
            reader.close();
            inputStream.close();
        } catch (IOException e) {
            fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + marker + "\t" + limit + "\t" + "{\"msg\":\"" + e.getMessage() + "\"}");
        } finally {
            if (response != null) {
                response.close();
            }
        }

        return endMarker.get();
    }

    public String doListV2(ListBucket listBucket, String bucket, String prefix, String marker, int limit, FileReaderAndWriterMap fileReaderAndWriterMap,
                           IOssFileProcess iOssFileProcessor, boolean processBatch, boolean withParallel, int retryCount) {

        AtomicReference<String> endMarker = new AtomicReference<>();
        try {
            Response response = listBucket.run(bucket, prefix, "", marker, limit, retryCount, 2);
            InputStream inputStream = new BufferedInputStream(response.bodyStream());
            Reader reader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(reader);
            Stream<String> lineStream = withParallel ? bufferedReader.lines().parallel() : bufferedReader.lines();
            lineStream.forEach(line -> {
                String[] firstFileInfoAndMarker = new String[3];
                try { firstFileInfoAndMarker = getFirstFileInfoAndMarker(null, line, null, 2);
                } catch (QiniuException e) {}
                String fileInfo = firstFileInfoAndMarker[1];
                String nextMarker = firstFileInfoAndMarker[2];
                fileReaderAndWriterMap.writeSuccess(fileInfo);
                endMarker.set(nextMarker);
                if (iOssFileProcessor != null) {
                    if (processBatch) iOssFileProcessor.batchProcessFile(fileInfo, retryCount);
                    else iOssFileProcessor.processFile(fileInfo, retryCount);
                }
            });
            bufferedReader.close();
            reader.close();
            inputStream.close();
            response.close();
        } catch (Exception e) {
            fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + prefix + "\t" + marker + "\t" + limit + "\t" + e.getMessage());
        }

        return endMarker.get();
    }

    public void processBucketWithEndFile(IOssFileProcess iOssFileProcessor, boolean processBatch, int version, int maxThreads,
                boolean withParallel, int level, int unitLen) throws IOException, CloneNotSupportedException {

        Map<String, String> delimitedFileMap = getDelimitedFileMap(version, level, iOssFileProcessor);
        List<String> keyPrefixList = new ArrayList<>(delimitedFileMap.keySet());
        Collections.sort(keyPrefixList);
        int runningThreads = delimitedFileMap.size() < maxThreads ? delimitedFileMap.size() : maxThreads;
        System.out.println("there are " + runningThreads + " threads running...");

        ExecutorService executorPool = Executors.newFixedThreadPool(runningThreads);
        for (int i = 0; i < keyPrefixList.size(); i++) {
            int finalI = i;
            FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap();
            fileMap.initWriter(resultFileDir, "list");
            IOssFileProcess processor = iOssFileProcessor != null ? iOssFileProcessor.clone() : null;
            executorPool.execute(() -> {
                String endFileKey = finalI == keyPrefixList.size() - 1 ? "" : keyPrefixList.get(finalI + 1);
                String marker = delimitedFileMap.get(keyPrefixList.get(finalI));
                ListBucket listBucket = new ListBucket(auth, configuration);
                while (!StringUtils.isNullOrEmpty(marker)) {
                    marker = version == 2 ?
                            doListV2(listBucket, bucket, marker, unitLen, endFileKey, fileMap, processor, processBatch, withParallel, 3) :
                            doListV1(listBucket, bucket, marker, unitLen, endFileKey, fileMap, processor, processBatch, 3);
                    System.out.println("endFileKey: " + endFileKey + ", marker: " + marker);
                }
                listBucket.closeBucketManager();
                if (processor != null) processor.closeResource();
                fileMap.closeWriter();
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

    public void processBucketWithPrefix(IOssFileProcess iOssFileProcessor, boolean processBatch, int version, int maxThreads,
                boolean withParallel, int level, int unitLen) throws IOException, CloneNotSupportedException {

        Map<String, String> delimitedFileMap = getDelimitedFileMap(version, level, iOssFileProcessor);
        List<String> keyPrefixList = new ArrayList<>(delimitedFileMap.keySet());
        Collections.sort(keyPrefixList);
        int runningThreads = delimitedFileMap.size() < maxThreads ? delimitedFileMap.size() : maxThreads;
        System.out.println("there are " + runningThreads + " threads running...");

        ExecutorService executorPool = Executors.newFixedThreadPool(runningThreads);
        for (String keyPrefix : keyPrefixList) {
            FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap();
            fileMap.initWriter(resultFileDir, "list");
            IOssFileProcess processor = iOssFileProcessor != null ? iOssFileProcessor.clone() : null;
            executorPool.execute(() -> {
                String prefix = level == 2 ? keyPrefix.substring(0,2) : keyPrefix.substring(0, 1);
                String marker = delimitedFileMap.get(keyPrefix);
                ListBucket listBucket = new ListBucket(auth, configuration);
                while (!StringUtils.isNullOrEmpty(marker)) {
                    marker = version == 2 ?
                            doListV2(listBucket, bucket, prefix, marker, unitLen, fileMap, processor, processBatch, withParallel, 3) :
                            doListV1(listBucket, bucket, prefix, marker, unitLen, fileMap, processor, processBatch, 3);
                    System.out.println("prefix: " + prefix + ", marker: " + marker);
                }
                listBucket.closeBucketManager();
                if (processor != null) processor.closeResource();
                fileMap.closeWriter();
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
        fileReaderAndWriterMap.closeWriter();
    }
}