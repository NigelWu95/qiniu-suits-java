package com.qiniu.service.datasource;

import com.google.gson.*;
import com.qiniu.common.*;
import com.qiniu.http.Response;
import com.qiniu.service.interfaces.IOssFileProcess;
import com.qiniu.model.ListResult;
import com.qiniu.model.ListV2Line;
import com.qiniu.sdk.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ListBucket {

    private Auth auth;
    private Configuration configuration;
    private String bucket;
    private int unitLen;
    private int version;
    private String resultFormat = "json";
    private String resultFileDir = "../result";
    private String customPrefix;
    private List<String> antiPrefix;
    private int retryCount;
    private ListFileFilter filter;
    private ListFileAntiFilter antiFilter;
    private boolean doFilter;
    private boolean doAntiFilter;
    private List<String> originPrefixList = Arrays.asList(
            " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
            .split(""));

    public ListBucket(Auth auth, Configuration configuration, String bucket, int unitLen, int version,
                      String customPrefix, List<String> antiPrefix, int retryCount) {
        this.auth = auth;
        this.configuration = configuration;
        this.bucket = bucket;
        this.unitLen = unitLen;
        this.version = version;
        this.customPrefix = customPrefix;
        this.antiPrefix = antiPrefix;
        this.retryCount = retryCount;
    }

    public void setResultParams(String resultFormat, String resultFileDir) {
        this.resultFormat = resultFormat;
        this.resultFileDir = resultFileDir;
    }

    public void setFilter(ListFileFilter listFileFilter, ListFileAntiFilter listFileAntiFilter) {
        this.filter = listFileFilter;
        this.antiFilter = listFileAntiFilter;
        this.doFilter = ListFileFilterUtils.checkListFileFilter(listFileFilter);
        this.doFilter = ListFileFilterUtils.checkListFileAntiFilter(listFileAntiFilter);
    }

    /*
    v2 的 list 接口，通过 IO 流的方式返回文本信息，v1 是单次请求的结果一次性返回。
     */
    public Response list(BucketManager bucketManager, String prefix, String delimiter, String marker, int limit)
            throws QiniuException {

        Response response = null;
        try {
            response = version == 2 ?
                    bucketManager.listV2(bucket, prefix, marker, limit, delimiter) :
                    bucketManager.listV1(bucket, prefix, marker, limit, delimiter);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    System.out.println("listV" + version + " " + bucket + ":" + prefix + ":" + marker + ":" + limit
                            + ":" + delimiter + " " + e1.error() + ", last " + retryCount + " times retry...");
                    response = version == 2 ?
                            bucketManager.listV2(bucket, prefix, marker, limit, delimiter) :
                            bucketManager.listV1(bucket, prefix, marker, limit, delimiter);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }

    private List<FileInfo> filterFileInfo(List<FileInfo> fileInfoList) {

        if (fileInfoList == null || fileInfoList.size() == 0) {
            return fileInfoList;
        } else if (doFilter && doAntiFilter) {
            return fileInfoList.parallelStream()
                    .filter(fileInfo -> filter.doFileFilter(fileInfo) && antiFilter.doFileAntiFilter(fileInfo))
                    .collect(Collectors.toList());
        } else if (doFilter) {
            return fileInfoList.parallelStream()
                    .filter(fileInfo -> filter.doFileFilter(fileInfo))
                    .collect(Collectors.toList());
        } else if (doAntiFilter) {
            return fileInfoList.parallelStream()
                    .filter(fileInfo -> antiFilter.doFileAntiFilter(fileInfo))
                    .collect(Collectors.toList());
        } else {
            return fileInfoList;
        }
    }

    private void writeResult(List<FileInfo> fileInfoList, FileReaderAndWriterMap fileMap, int writeType) {

        if (fileInfoList == null || fileInfoList.size() == 0) return;
        if (fileMap != null) {
            Stream<FileInfo> fileInfoStream = fileInfoList.parallelStream().filter(Objects::nonNull);
            List<String> list = resultFormat.equals("json") ?
                    fileInfoStream.map(JsonConvertUtils::toJsonWithoutUrlEscape).collect(Collectors.toList()) :
                    fileInfoStream.map(LineUtils::toSeparatedItemLine).collect(Collectors.toList());
            if (writeType == 1) fileMap.writeSuccess(String.join("\n", list));
            if (writeType == 2) fileMap.writeOther(String.join("\n", list));
        }
    }

    public ListV2Line getItemByList2Line(String line) {

        ListV2Line listV2Line = new ListV2Line();
        if (!StringUtils.isNullOrEmpty(line)) {
            JsonObject json = new JsonObject();
            // to test the exceptional line.
            try {
                json = JsonConvertUtils.toJsonObject(line);
            } catch (JsonParseException e) {
                System.out.println(line);
                e.printStackTrace();
            }
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
                listResult.fileInfoList = listV2LineList.parallelStream()
                        .map(listV2Line -> listV2Line.fileInfo)
                        .collect(Collectors.toList());
                Optional<ListV2Line> lastListV2Line = listV2LineList.parallelStream()
                        .max(ListV2Line::compareTo);
                lastListV2Line.ifPresent(listV2Line -> listResult.nextMarker = listV2Line.marker);
            }
        }

        return listResult;
    }

    private List<ListResult> preListByPrefix(BucketManager bucketManager, List<String> prefixList, int unitLen,
                                             String resultPrefix) throws IOException {
        FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap();
        fileMap.initWriter(resultFileDir, resultPrefix, "pre");
        List<ListResult> listResultList = prefixList.parallelStream()
                .map(prefix -> {
                    Response response = null;
                    ListResult listResult = new ListResult();
                    try {
                        response = list(bucketManager, prefix, null, null, unitLen);
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

    public List<ListResult> preList(int unitLen, int level, String customPrefix, List<String> antiPrefix,
                                    String resultPrefix) throws IOException {
        List<String> validPrefixList = originPrefixList.parallelStream()
                .filter(originPrefix -> !antiPrefix.contains(originPrefix))
                .map(prefix -> StringUtils.isNullOrEmpty(customPrefix) ? prefix : customPrefix + prefix)
                .collect(Collectors.toList());
        List<ListResult> listResultList = new ArrayList<>();
        BucketManager bucketManager = new BucketManager(auth, configuration);
        if (level == 1) {
            listResultList = preListByPrefix(bucketManager, validPrefixList, unitLen, resultPrefix);
        } else if (level == 2) {
            listResultList = preListByPrefix(bucketManager, validPrefixList, 1, resultPrefix);
            List<String> level2PrefixList = listResultList.parallelStream()
                    .map(singlePrefixListResult -> originPrefixList.parallelStream()
                            .filter(originPrefix -> !antiPrefix.contains(originPrefix))
                            .map(originPrefix -> singlePrefixListResult.commonPrefix + originPrefix)
                            .collect(Collectors.toList()))
                    .reduce((list1, list2) -> {
                        list1.addAll(list2);
                        return list1;
                    }).get();
            listResultList = preListByPrefix(bucketManager, level2PrefixList, unitLen, resultPrefix);
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

    private void loopList(BucketManager bucketManager, String prefix, String endFile, String marker,
                          FileReaderAndWriterMap fileMap, IOssFileProcess processor, boolean processBatch) {
        recordProgress(prefix, endFile, marker, fileMap);
        List<FileInfo> fileInfoList = new ArrayList<>();
        boolean needRedo = true;
        while (!StringUtils.isNullOrEmpty(marker)) {
            try {
                if (needRedo) {
                    marker = "null".equals(marker) ? "" : marker;
                    Response response = list(bucketManager, prefix, "", marker, unitLen);
                    ListResult listResult = getListResult(response, version);
                    response.close();
                    marker = !StringUtils.isNullOrEmpty(endFile) && listResult.fileInfoList.parallelStream()
                            .anyMatch(fileInfo -> fileInfo != null && endFile.compareTo(fileInfo.key) <= 0) ?
                            "" : listResult.nextMarker;
                    fileInfoList = listResult.fileInfoList.parallelStream().filter(fileInfo -> fileInfo != null &&
                            (StringUtils.isNullOrEmpty(endFile) || fileInfo.key.compareTo(endFile) <= 0))
                            .collect(Collectors.toList());
                    writeResult(fileInfoList, fileMap, 1);
                    if (doFilter || doAntiFilter) {
                        fileInfoList = filterFileInfo(fileInfoList);
                        writeResult(fileInfoList, fileMap, 2);
                    }
                    recordProgress(prefix, endFile, marker, fileMap);
                }

                try {
                    if (processor != null && fileInfoList != null && fileInfoList.size() > 0)
                        processor.processFile(fileInfoList.parallelStream()
                                .filter(Objects::nonNull).collect(Collectors.toList()), processBatch, retryCount);
                    needRedo = true;
                } catch (QiniuException e) {
                    needRedo = false;
                    throw e;
                }
            } catch (QiniuException e) {
                e.printStackTrace();
                System.out.println(prefix + "\t" + endFile + "\t" + marker + "\t" + e.error());
                fileMap.writeErrorOrNull(prefix + "\t" + endFile + "\t" + marker + "\t" + e.error());
                fileMap.flushErrorOrNull();
                e.response.close();
            }
        }
    }

    public void straightlyList(String prefix, String endFile, String marker, IOssFileProcess iOssFileProcessor,
                             boolean processBatch) throws IOException {
        FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap();
        fileMap.initWriter(resultFileDir, "list", "total");
        BucketManager bucketManager = new BucketManager(auth, configuration);
        marker = StringUtils.isNullOrEmpty(marker) ? "null" : marker;
        loopList(bucketManager, prefix, endFile, marker, fileMap, iOssFileProcessor, processBatch);
        fileMap.closeWriter();
        System.out.println("list finished");
    }

    private void listTotalWithPrefix(ExecutorService pool, List<ListResult> resultList, IOssFileProcess fileProcessor,
                                     boolean processBatch) throws IOException, CloneNotSupportedException {
        resultList.sort(Comparator.comparing(listResult -> listResult.commonPrefix));
        for (int i = StringUtils.isNullOrEmpty(customPrefix) ? -1 : 0; i < resultList.size(); i++) {
            int finalI = i;
            int resultIndex = StringUtils.isNullOrEmpty(customPrefix) ? i + 2 : i + 1;
            FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap();
            fileMap.initWriter(resultFileDir, "list", resultIndex);
            IOssFileProcess processor = fileProcessor != null ? fileProcessor.getNewInstance(resultIndex) : null;
            List<FileInfo> fileInfoList = i > -1 ? resultList.get(i).fileInfoList.parallelStream()
                    .filter(Objects::nonNull).collect(Collectors.toList()) : null;
            writeResult(fileInfoList, fileMap, 1);
            if (doFilter || doAntiFilter) {
                fileInfoList = filterFileInfo(fileInfoList);
                writeResult(fileInfoList, fileMap, 2);
            }
            if (fileProcessor != null && fileInfoList != null && fileInfoList.size() > 0)
                fileProcessor.processFile(fileInfoList.parallelStream()
                    .filter(Objects::nonNull).collect(Collectors.toList()), processBatch, retryCount);
            pool.execute(() -> {
                String endFilePrefix = "";
                String prefix = "";
                String marker = "null";
                if (finalI < resultList.size() -1 && finalI > -1) {
                    prefix = resultList.get(finalI).commonPrefix;
                    marker = resultList.get(finalI).nextMarker;
                } else {
                    if (finalI == -1) endFilePrefix = resultList.get(0).commonPrefix;
                    else {
                        marker = resultList.get(finalI).nextMarker;
                        marker = !StringUtils.isNullOrEmpty(marker) ? marker :
                                ListBucketUtils.calcMarker(resultList.get(finalI).fileInfoList.parallelStream()
                                        .max(Comparator.comparing(fileInfo1 -> fileInfo1.key)).get());
                    }
                    if (!StringUtils.isNullOrEmpty(customPrefix)) prefix = customPrefix;
                }
                BucketManager bucketManager = new BucketManager(auth, configuration);
                loopList(bucketManager, prefix, endFilePrefix, marker, fileMap, processor, processBatch);
                if (processor != null) processor.closeResource();
                fileMap.closeWriter();
            });
        }
    }

    public void concurrentlyList(int maxThreads, int level, IOssFileProcess processor, boolean processBatch)
            throws IOException, CloneNotSupportedException {

        List<ListResult> listResultList = preList(unitLen, level, customPrefix, antiPrefix, "list");
        int listSize = listResultList.size();
        int runningThreads = StringUtils.isNullOrEmpty(customPrefix) ? listSize + 1 : listSize;
        runningThreads = runningThreads < maxThreads ? runningThreads : maxThreads;
        String info = "list bucket " + (processor == null ? "" : "and " + processor.getProcessName());
        System.out.println(info + " concurrently running with " + runningThreads + " threads ...");
        ExecutorService executorPool = Executors.newFixedThreadPool(runningThreads);
        try {
            listTotalWithPrefix(executorPool, listResultList, processor, processBatch);
        } finally {
            executorPool.shutdown();
            try {
                while (!executorPool.isTerminated())
                    Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println(info + " finished");
    }
}
