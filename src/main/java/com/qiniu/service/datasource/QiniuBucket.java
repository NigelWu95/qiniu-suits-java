package com.qiniu.service.datasource;

import com.google.gson.JsonObject;
import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.ListFileAntiFilter;
import com.qiniu.common.ListFileFilter;
import com.qiniu.common.QiniuException;
import com.qiniu.sdk.BucketManager;
import com.qiniu.service.interfaces.IOssFileProcess;
import com.qiniu.service.oss.FileLister;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QiniuBucket {

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

    public QiniuBucket(Auth auth, Configuration configuration, String bucket, int unitLen, int version,
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
        this.doAntiFilter = ListFileFilterUtils.checkListFileAntiFilter(listFileAntiFilter);
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

    private List<FileLister> prefixList(List<String> prefixList, int unitLen) {

        return prefixList.parallelStream()
                .map(prefix -> {
                    try {
                        return new FileLister(new BucketManager(auth, configuration), bucket, prefix, null,
                                null, unitLen, version, retryCount);
                    } catch (QiniuException e) {
                        throw new RuntimeException(prefix + "\t" + e.error(), e);
                    }
                })
                .collect(Collectors.toList());
    }

    private List<FileLister> getFileListerList(int unitLen, int level) {
        List<String> validPrefixList = originPrefixList.parallelStream()
                .filter(originPrefix -> !antiPrefix.contains(originPrefix))
                .map(prefix -> StringUtils.isNullOrEmpty(customPrefix) ? prefix : customPrefix + prefix)
                .collect(Collectors.toList());
        List<FileLister> fileListerList = new ArrayList<>();

        if (level == 1) {
            fileListerList = prefixList(validPrefixList, unitLen);
        } else if (level == 2) {
            fileListerList = prefixList(validPrefixList, 1);
            List<String> level2PrefixList = fileListerList.parallelStream()
                    .map(singlePrefixFileLister -> originPrefixList.parallelStream()
                    .filter(originPrefix -> !antiPrefix.contains(originPrefix))
                    .map(originPrefix -> singlePrefixFileLister.getPrefix() + originPrefix)
                    .collect(Collectors.toList()))
                    .reduce((list1, list2) -> { list1.addAll(list2); return list1; })
                    .orElse(validPrefixList);
            fileListerList = prefixList(level2PrefixList, unitLen);
        }

        return fileListerList;
    }

    private void recordProgress(String prefix, String marker, String endFile, FileReaderAndWriterMap fileMap) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("prefix", prefix);
        jsonObject.addProperty("marker", marker);
        jsonObject.addProperty("end", endFile);
        fileMap.writeKeyFile("marker" + fileMap.getSuffix(), JsonConvertUtils.toJsonWithoutUrlEscape(jsonObject));
    }

    private void listFilesWithProcess(FileLister fileLister, String prefix, String marker, String endFile,
                           FileReaderAndWriterMap fileMap, IOssFileProcess processor) throws QiniuException {
        recordProgress(prefix, marker, endFile, fileMap);
        List<FileInfo> fileInfoList;
        while (fileLister.hasNext()) {
            fileInfoList = fileLister.next();
            marker = fileLister.getMarker();
            if (!StringUtils.isNullOrEmpty(endFile)) {
                marker = fileInfoList.parallelStream()
                        .anyMatch(fileInfo -> fileInfo != null && endFile.compareTo(fileInfo.key) <= 0)
                        ? null : marker;
                fileInfoList = fileInfoList.parallelStream()
                        .filter(fileInfo -> fileInfo != null && fileInfo.key.compareTo(endFile) <= 0)
                        .collect(Collectors.toList());
            }
            writeResult(fileInfoList, fileMap, 1);
            if (doFilter || doAntiFilter) {
                fileInfoList = filterFileInfo(fileInfoList);
                writeResult(fileInfoList, fileMap, 2);
            }
            recordProgress(prefix, marker, endFile, fileMap);
            if (processor != null) processor.processFile(fileInfoList.parallelStream()
                    .filter(Objects::nonNull).collect(Collectors.toList()), retryCount);
            if (StringUtils.isNullOrEmpty(marker)) break;
        }
        if (fileLister.exception != null) throw fileLister.exception;
    }

    private Map<String, String> calcListParams(List<FileLister> resultList, int finalI) {
        String prefix = "";
        String marker = "null";
        String end = "";
        if (finalI < resultList.size() -1 && finalI > -1) {
            prefix = resultList.get(finalI).getPrefix();
            marker = resultList.get(finalI).getMarker();
        } else {
            if (finalI == -1) end = resultList.get(0).getPrefix();
            else {
                marker = resultList.get(finalI).getMarker();
                FileLister fileLister = resultList.get(finalI);
                if (StringUtils.isNullOrEmpty(marker) && fileLister.hasNext()) {
                    FileInfo lastFileInfo = fileLister.next().parallelStream()
                            .max(Comparator.comparing(fileInfo -> fileInfo.key))
                            .orElse(null);
                    marker = ListBucketUtils.calcMarker(lastFileInfo);
                }
            }
            if (!StringUtils.isNullOrEmpty(customPrefix)) prefix = customPrefix;
        }

        String finalPrefix = prefix;
        String finalMarker = marker;
        String finalEnd = end;
        return new HashMap<String, String>(){{
            put("prefix", finalPrefix);
            put("marker", finalMarker);
            put("end", finalEnd);
        }};
    }

    private void list(int finalI, List<FileLister> resultList, IOssFileProcess fileProcessor) {
        int resultIndex = StringUtils.isNullOrEmpty(customPrefix) ? finalI + 2 : finalI + 1;
        FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap();
        try {
            fileMap.initWriter(resultFileDir, "list", resultIndex);
            IOssFileProcess processor = fileProcessor != null ? fileProcessor.getNewInstance(resultIndex) : null;
            if (finalI > -1) {
                List<FileInfo> fileInfoList = resultList.get(finalI).next().parallelStream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                writeResult(fileInfoList, fileMap, 1);
                if (doFilter || doAntiFilter) {
                    fileInfoList = filterFileInfo(fileInfoList);
                    writeResult(fileInfoList, fileMap, 2);
                }
                if (processor != null) processor.processFile(fileInfoList, retryCount);
            }

            Map<String, String> params = calcListParams(resultList, finalI);
            String prefix = params.get("prefix");
            String marker = params.get("marker");
            String endFilePrefix = params.get("end");
            BucketManager bucketManager = new BucketManager(auth, configuration);
            FileLister fileLister = new FileLister(bucketManager, bucket, prefix, "", marker, unitLen,
                    version, retryCount);
            listFilesWithProcess(fileLister, prefix, marker, endFilePrefix, fileMap, processor);
            if (processor != null) processor.closeResource();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            fileMap.closeWriter();
        }
    }

    public void concurrentlyList(int maxThreads, int level, IOssFileProcess processor) {
        List<FileLister> fileListerList = getFileListerList(unitLen, level);
        int listSize = fileListerList.size();
        int runningThreads = StringUtils.isNullOrEmpty(customPrefix) ? listSize + 1 : listSize;
        runningThreads = runningThreads < maxThreads ? runningThreads : maxThreads;
        String info = "list bucket" + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " concurrently running with " + runningThreads + " threads ...");
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable);
            thread.setUncaughtExceptionHandler((t, e) -> System.out.println(t.getName() + "\t" + e.getMessage()));
            return thread;
        };
        ExecutorService executorPool = Executors.newFixedThreadPool(runningThreads, threadFactory);
        fileListerList.sort(Comparator.comparing(FileLister::getPrefix));
        for (int i = StringUtils.isNullOrEmpty(customPrefix) ? -1 : 0; i < fileListerList.size(); i++) {
            int finalI = i;
            executorPool.execute(() -> list(finalI, fileListerList, processor));
        }
        executorPool.shutdown();
        try {
            while (!executorPool.isTerminated()) Thread.sleep(1000);
            System.out.println(info + " finished");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void checkValidPrefix(int level) {
        List<FileLister> fileListerList = getFileListerList(1, level);
        FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap();
        try {
            fileMap.initWriter(resultFileDir, "list", "check");
            List<String> validPrefixAndMarker = fileListerList.parallelStream()
                    .map(fileLister -> fileLister.getPrefix() + "\t" + fileLister.getMarker())
                    .collect(Collectors.toList());
            fileMap.writeSuccess(String.join("\n", validPrefixAndMarker));
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            fileMap.closeWriter();
        }
    }

    public void straightlyList(String marker, String endFile, IOssFileProcess processor) {
        FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap();
        try {
            String info = "list bucket" + (processor == null ? "" : " and " + processor.getProcessName());
            System.out.println(info + " start...");
            fileMap.initWriter(resultFileDir, "list", "total");
            BucketManager bucketManager = new BucketManager(auth, configuration);
            marker = StringUtils.isNullOrEmpty(marker) ? "null" : marker;
            FileLister fileLister = new FileLister(bucketManager, bucket, customPrefix, "", marker, unitLen,
                    version, retryCount);
            listFilesWithProcess(fileLister, customPrefix, marker, endFile, fileMap, processor);
            System.out.println(info + " finished.");
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            fileMap.closeWriter();
        }
    }
}