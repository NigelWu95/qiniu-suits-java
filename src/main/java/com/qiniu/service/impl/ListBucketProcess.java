package com.qiniu.service.impl;

import com.google.gson.*;
import com.qiniu.common.*;
import com.qiniu.common.QiniuBucketManager.*;
import com.qiniu.http.Response;
import com.qiniu.interfaces.IBucketProcess;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.service.oss.ListBucket;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.JSONConvertUtils;
import com.qiniu.util.Json;
import com.qiniu.util.StringUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class ListBucketProcess implements IBucketProcess {

    private QiniuAuth auth;
    private Configuration configuration;
    private String bucket;
    private FileReaderAndWriterMap fileReaderAndWriterMap = new FileReaderAndWriterMap();

    public ListBucketProcess(QiniuAuth auth, Configuration configuration, String bucket, String resultFileDir)
            throws IOException {

        this.bucket = bucket;
        fileReaderAndWriterMap.initWriter(resultFileDir, "list");
        this.auth = auth;
        this.configuration = configuration;
    }

    public String[] getFirstFileInfoAndMarkerV2(String line) {

        if (StringUtils.isNullOrEmpty(line))
            return new String[]{"", "", ""};
        JsonObject json = JSONConvertUtils.toJson(line);
        JsonElement item = json.get("item");
        String fileKey = "";
        String fileInfo = "";
        String nextMarker = json.get("marker").getAsString();
//        String dir = json.get("dir").getAsString();

        if (item != null && !(item instanceof JsonNull)) {
            fileKey = item.getAsJsonObject().get("key").getAsString();
            fileInfo = JSONConvertUtils.toJson(json.getAsJsonObject("item"));
        }

        return new String[]{fileKey, fileInfo, nextMarker};
    }

    public String[] getFirstFileInfoAndMarkerV1(FileListing fileListing) {

        FileInfo[] items = fileListing.items;
        String nextMarker = fileListing.marker;
        String fileKey = "";
        String fileInfo = "";

        if (items.length > 0) {
            fileKey = items[0].key;
            fileInfo = JSONConvertUtils.toJson(items[0]);
        }

        return new String[]{fileKey, fileInfo, nextMarker};
    }

    private Map<String, String> listByPrefix(ListBucket listBucket, List<String> prefixList, int version, boolean doWrite,
                                             boolean doProcess, IOssFileProcess iOssFileProcessor) throws QiniuException {
        Map<String, String> delimitedFileMap = new HashMap<>();

        for (String prefix : prefixList) {
            Response response = null;
            String[] firstFileInfoAndMarker = new String[]{};
            try {
                response = listBucket.run(bucket, prefix, null, null, 1, 3, version);
                if (version == 1) {
                    FileListing fileListing = response.jsonToObject(FileListing.class);
                    firstFileInfoAndMarker = getFirstFileInfoAndMarkerV1(fileListing);
                } else if (version == 2) {
                    String line = response.bodyString();
                    firstFileInfoAndMarker = getFirstFileInfoAndMarkerV2(line);
                }
            } catch (QiniuException e) {
                fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + prefix + "\t" + e.error());
                if (e.code() > 400) throw e; else continue;
            } catch (NullPointerException e) {
                fileReaderAndWriterMap.writeErrorOrNull( bucket + "\t" + prefix + "\tnull exception: " + e.getMessage());
                continue;
            } finally {
                if (response != null)
                    response.close();
            }

            String fileKey = firstFileInfoAndMarker[0];
            String fileInfo = firstFileInfoAndMarker[1];
            String nextMarker = firstFileInfoAndMarker[2];
            if (StringUtils.isNullOrEmpty(fileKey) || delimitedFileMap.keySet().contains(fileKey))
                continue;
            if (doWrite) {
                fileReaderAndWriterMap.writeSuccess(fileInfo);
                if (doProcess) {
                    iOssFileProcessor.processFile(fileInfo, 3);
                    if (iOssFileProcessor.qiniuException() != null && iOssFileProcessor.qiniuException().code() > 400)
                        throw iOssFileProcessor.qiniuException();
                }
            }
            delimitedFileMap.put(fileKey, nextMarker);
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
            delimitedFileMap = listByPrefix(listBucket, prefixList, version, false, doProcess, iOssFileProcessor);
            prefixList = getSecondFilePrefix(prefixList, delimitedFileMap);
            delimitedFileMap.putAll(listByPrefix(listBucket, prefixList, version, true, doProcess, iOssFileProcessor));
        } else {
            delimitedFileMap = listByPrefix(listBucket, prefixList, version, true, doProcess, iOssFileProcessor);
        }

        listBucket.closeBucketManager();

        return delimitedFileMap;
    }

    /*
    单次列举，可以传递 marker 和 limit 参数，通常采用此方法进行并发处理
     */
    public String doListV1(ListBucket listBucket, String bucket, String prefix, String marker, int limit,
                           IOssFileProcess iOssFileProcessor, int retryCount) {

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
                iOssFileProcessor.processFile(fileInfo, retryCount);
            }

            return fileListing.marker;
        }

    }

    public String doListV1(ListBucket listBucket, String bucket, String marker, int limit, String endFile,
                           IOssFileProcess iOssFileProcessor, int retryCount) {

        FileListing fileListing = doListV1(listBucket, bucket, "", "", marker, limit, false, retryCount);
        FileInfo[] items = fileListing.items;
        String fileInfo;

        for (FileInfo item : items) {
            if (item.key.equals(endFile)) return null;
            fileInfo = JSONConvertUtils.toJson(item);
            fileReaderAndWriterMap.writeSuccess(fileInfo);
            if (iOssFileProcessor != null)
                iOssFileProcessor.processFile(fileInfo, retryCount);
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
            fileReaderAndWriterMap.writeErrorOrNull(bucket + "\t" + prefix + "\t" + delimiter + "\t" + marker
                    + "\t" + limit + "\t" + e.getMessage());
        } finally {
            if (response != null)
                response.close();
        }

        return fileListing;
    }

    /*
    迭代器方式列举带 prefix 前缀的所有文件，直到列举完毕，limit 为单次列举的文件个数
     */
    public void doIteratorListV1(String bucket, String prefix, String endFile, int limit, IOssFileProcess iOssFileProcessor) {

        QiniuBucketManager bucketManager = new QiniuBucketManager(auth, configuration);
        FileListIterator fileListIterator = bucketManager.createFileListIterator(bucket, prefix, limit, null);

        loop:while (fileListIterator.hasNext()) {
            FileInfo[] items = fileListIterator.next();

            for (FileInfo fileInfo : items) {
                if (fileInfo.key.equals(endFile)) {
                    break loop;
                }
                fileReaderAndWriterMap.writeSuccess(JSONConvertUtils.toJson(fileInfo));
                if (iOssFileProcessor != null) {
                    iOssFileProcessor.processFile(JSONConvertUtils.toJson(fileInfo), 3);
                }
            }
        }
    }

    /*
    v2 的 list 接口，接收到响应后通过 java8 的流来处理响应的文本流。
     */
    public String doListV2(ListBucket listBucket, String bucket, String marker, int limit, String endFile,
                           IOssFileProcess iOssFileProcessor, boolean withParallel, int retryCount) {

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
                String[] firstFileInfoAndMarker = getFirstFileInfoAndMarkerV2(line);
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
                    if (iOssFileProcessor != null)
                        iOssFileProcessor.processFile(fileInfo, retryCount);
                }
            });
            bufferedReader.close();
            reader.close();
            inputStream.close();
        } catch (IOException e) {
            fileReaderAndWriterMap.writeOther(bucket + "\t" + marker + "\t" + limit + "\t" + "{\"msg\":\"" + e.getMessage() + "\"}");
        } finally {
            if (response != null) {
                response.close();
            }
        }

        return endMarker.get();
    }

    public String doListV2(ListBucket listBucket, String bucket, String prefix, String marker, int limit,
                           IOssFileProcess iOssFileProcessor, boolean withParallel, int retryCount) {

        Response response = null;
        AtomicReference<String> endMarker = new AtomicReference<>();

        try {
            response = listBucket.run(bucket, prefix, "", marker, limit, retryCount, 2);
            InputStream inputStream = new BufferedInputStream(response.bodyStream());
            Reader reader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(reader);
            Stream<String> lineStream = withParallel ? bufferedReader.lines().parallel() : bufferedReader.lines();
            lineStream.forEach(line -> {
                String[] firstFileInfoAndMarker = getFirstFileInfoAndMarkerV2(line);
                String fileInfo = firstFileInfoAndMarker[1];
                String nextMarker = firstFileInfoAndMarker[2];
                fileReaderAndWriterMap.writeSuccess(fileInfo);
                endMarker.set(nextMarker);
                if (iOssFileProcessor != null)
                    iOssFileProcessor.processFile(fileInfo, retryCount);
            });
            bufferedReader.close();
            reader.close();
            inputStream.close();
        } catch (IOException e) {
            fileReaderAndWriterMap.writeOther(bucket + "\t" + prefix + "\t" + marker + "\t" + limit + "\t" + e.getMessage());
        } finally {
            if (response != null) {
                response.close();
            }
        }

        return endMarker.get();
    }

    public void processBucketWithEndFile(IOssFileProcess iOssFileProcessor, int version, int maxThreads, boolean withParallel,
                             int level, int unitLen) throws QiniuException {

        Map<String, String> delimitedFileMap = getDelimitedFileMap(version, level, iOssFileProcessor);
        List<String> keyPrefixList = new ArrayList<>(delimitedFileMap.keySet());
        Collections.sort(keyPrefixList);
        int runningThreads = delimitedFileMap.size() < maxThreads ? delimitedFileMap.size() : maxThreads;
        System.out.println("there are " + runningThreads + " threads running...");

        ExecutorService executorPool = Executors.newFixedThreadPool(runningThreads);
        for (int i = 0; i < keyPrefixList.size(); i++) {
            int finalI = i;
            executorPool.execute(() -> {
                String endFileKey = finalI == keyPrefixList.size() - 1 ? "" : keyPrefixList.get(finalI + 1);
                String marker = delimitedFileMap.get(keyPrefixList.get(finalI));
                ListBucket listBucket = new ListBucket(auth, configuration);
                while (!StringUtils.isNullOrEmpty(marker)) {
                    marker = version == 2 ?
                            doListV2(listBucket, bucket, marker, unitLen, endFileKey, iOssFileProcessor, withParallel, 3) :
                            doListV1(listBucket, bucket, marker, unitLen, endFileKey, iOssFileProcessor, 3);
                    System.out.println("endFileKey: " + endFileKey + ", marker: " + marker);
                }
                listBucket.closeBucketManager();
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

    public void processBucketWithPrefix(IOssFileProcess iOssFileProcessor, int version, int maxThreads, boolean withParallel,
                                        int level, int unitLen) throws QiniuException {

        Map<String, String> delimitedFileMap = getDelimitedFileMap(version, level, iOssFileProcessor);
        List<String> keyPrefixList = new ArrayList<>(delimitedFileMap.keySet());
        Collections.sort(keyPrefixList);
        int runningThreads = delimitedFileMap.size() < maxThreads ? delimitedFileMap.size() : maxThreads;
        System.out.println("there are " + runningThreads + " threads running...");

        ExecutorService executorPool = Executors.newFixedThreadPool(runningThreads);
        for (String keyPrefix : keyPrefixList) {
            executorPool.execute(() -> {
                String prefix = level == 2 ? keyPrefix.substring(0,2) : keyPrefix.substring(0, 1);
                String marker = delimitedFileMap.get(keyPrefix);
                ListBucket listBucket = new ListBucket(auth, configuration);
                while (!StringUtils.isNullOrEmpty(marker)) {
                    marker = version == 2 ?
                            doListV2(listBucket, bucket, prefix, marker, unitLen, iOssFileProcessor, withParallel, 3) :
                            doListV1(listBucket, bucket, prefix, marker, unitLen, iOssFileProcessor, 3);
                    System.out.println("prefix: " + prefix + ", marker: " + marker);
                }
                listBucket.closeBucketManager();
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