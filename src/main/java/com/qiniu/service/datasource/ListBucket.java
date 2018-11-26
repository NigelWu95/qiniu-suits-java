package com.qiniu.service.datasource;

import com.google.gson.JsonObject;
import com.qiniu.common.FileMap;
import com.qiniu.common.ListFileAntiFilter;
import com.qiniu.common.ListFileFilter;
import com.qiniu.common.QiniuException;
import com.qiniu.sdk.BucketManager;
import com.qiniu.service.interfaces.IQossProcess;
import com.qiniu.service.qoss.FileLister;
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
        this.customPrefix = customPrefix == null ? "" : customPrefix;
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

    private void writeResult(List<FileInfo> fileInfoList, FileMap fileMap, int writeType) {

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
                        throw new RuntimeException(prefix + "\t" + e.error(), e.getCause());
                    }
                })
                .filter(FileLister::hasNext)
                .collect(Collectors.toList());
    }

    private List<FileLister> getFileListerList(int unitLen, int level) {
        String cPrefix = customPrefix;
        List<String> validPrefixList = originPrefixList.parallelStream()
                .filter(originPrefix -> !antiPrefix.contains(originPrefix))
                .map(prefix -> cPrefix + prefix)
                .collect(Collectors.toList());
        validPrefixList.add(cPrefix);
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
            level2PrefixList.add(cPrefix);
            fileListerList = prefixList(level2PrefixList, unitLen);
        }

        return fileListerList;
    }

    private void recordProgress(String prefix, String marker, String endFile, FileMap fileMap) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("prefix", prefix);
        jsonObject.addProperty("marker", marker);
        jsonObject.addProperty("end", endFile);
        fileMap.writeKeyFile("marker" + fileMap.getSuffix(), JsonConvertUtils.toJsonWithoutUrlEscape(jsonObject));
    }

    private void seekListerToEnd(FileLister fileLister, String endFile, FileMap fileMap, IQossProcess processor)
            throws QiniuException {
        List<FileInfo> fileInfoList;
        while (fileLister.hasNext() || fileLister.getPrefix().equals(customPrefix)) {
            String marker = fileLister.getMarker();
            recordProgress(fileLister.getPrefix(), marker, endFile, fileMap);
            fileInfoList = fileLister.next().parallelStream().filter(Objects::nonNull).collect(Collectors.toList());
            if (fileLister.exception != null) {
                HttpResponseUtils.processException(fileLister.exception, fileMap, "list", marker);
                continue;
            }
            if (!StringUtils.isNullOrEmpty(endFile)) {
                marker = fileInfoList.parallelStream()
                        .anyMatch(fileInfo -> endFile.compareTo(fileInfo.key) <= 0)
                        ? null : marker;
                fileInfoList = fileInfoList.parallelStream()
                        .filter(fileInfo -> fileInfo.key.compareTo(endFile) <= 0)
                        .collect(Collectors.toList());
            }
            writeResult(fileInfoList, fileMap, 1);
            if (doFilter || doAntiFilter) {
                fileInfoList = filterFileInfo(fileInfoList);
                writeResult(fileInfoList, fileMap, 2);
            }
            if (processor != null) processor.processFile(fileInfoList.parallelStream()
                    .filter(Objects::nonNull).collect(Collectors.toList()));
            if (StringUtils.isNullOrEmpty(marker)) break;
        }
    }

    private Map<String, String> calcListParams(List<FileLister> resultList, int finalI) {
        String prefix = resultList.get(finalI).getPrefix();
        String marker = resultList.get(finalI).getMarker();
        String end = "";
        if (resultList.size() > 1) {
            if (finalI == 0) {
                marker = "";
                end = resultList.get(1).getPrefix();
            } else if (finalI == resultList.size() -1) {
                prefix = customPrefix;
                FileLister fileLister = resultList.get(finalI);
                if (StringUtils.isNullOrEmpty(marker)) {
                    FileInfo lastFileInfo = fileLister.getFileInfoList().parallelStream()
                            .filter(Objects::nonNull)
                            .max(Comparator.comparing(fileInfo -> fileInfo.key))
                            .orElse(null);
                    marker = ListBucketUtils.calcMarker(lastFileInfo);
                }
            }
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

    private void listFromLister(int finalI, List<FileLister> fileListerList, IQossProcess fileProcessor) {
        int resultIndex = finalI + 1;
        FileMap fileMap = new FileMap();
        IQossProcess processor = null;
        try {
            fileMap.initWriter(resultFileDir, "list", resultIndex);
            if (fileProcessor != null) processor = fileProcessor.getNewInstance(resultIndex);
            Map<String, String> params = calcListParams(fileListerList, finalI);
            String prefix = params.get("prefix");
            String marker = params.get("marker");
            String endFilePrefix = params.get("end");
            FileLister fileLister = fileListerList.get(finalI);
            fileLister.setPrefix(prefix);
            fileLister.setMarker(marker);
            seekListerToEnd(fileLister, endFilePrefix, fileMap, processor);
        } catch (Exception e) {
            throw new RuntimeException(e.getCause());
        } finally {
            fileMap.closeWriter();
            if (processor != null) processor.closeResource();
        }
    }

    public void concurrentlyList(int maxThreads, int level, IQossProcess processor) {
        List<FileLister> fileListerList = getFileListerList(unitLen, level);
        int listSize = fileListerList.size();
        int runningThreads = listSize < maxThreads ? listSize : maxThreads;
        String info = "list bucket" + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " concurrently running with " + runningThreads + " threads ...");
        fileListerList.sort(Comparator.comparing(FileLister::getPrefix));
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable);
            thread.setUncaughtExceptionHandler((t, e) -> System.out.println(t.getName() + "\t" + e.getMessage()));
            return thread;
        };
        ExecutorService executorPool = Executors.newFixedThreadPool(runningThreads, threadFactory);
        for (int i = 0; i < fileListerList.size(); i++) {
            int finalI = i;
            executorPool.execute(() -> listFromLister(finalI, fileListerList, processor));
        }
        executorPool.shutdown();
        ExecutorsUtils.waitForShutdown(executorPool, info);
    }

    public void checkValidPrefix(int level) {
        List<FileLister> fileListerList = getFileListerList(1, level);
        FileMap fileMap = new FileMap();
        try {
            fileMap.initWriter(resultFileDir, "list", "check");
            List<String> validPrefixAndMarker = fileListerList.parallelStream()
                    .filter(FileLister::hasNext)
                    .map(fileLister -> fileLister.getPrefix() + "\t" + fileLister.getMarker())
                    .collect(Collectors.toList());
            fileMap.writeSuccess(String.join("\n", validPrefixAndMarker));
        } catch (IOException e) {
            throw new RuntimeException(e.getCause());
        } finally {
            fileMap.closeWriter();
        }
    }

    public void straightlyList(String marker, String endFile, IQossProcess processor) {
        FileMap fileMap = new FileMap();
        try {
            String info = "list bucket" + (processor == null ? "" : " and " + processor.getProcessName());
            System.out.println(info + " start...");
            fileMap.initWriter(resultFileDir, "list", "total");
            BucketManager bucketManager = new BucketManager(auth, configuration);
            FileLister fileLister = new FileLister(bucketManager, bucket, customPrefix, "", marker, unitLen,
                    version, retryCount);
            seekListerToEnd(fileLister, endFile, fileMap, processor);
            System.out.println(info + " finished.");
        } catch (IOException e) {
            throw new RuntimeException(e.getCause());
        } finally {
            fileMap.closeWriter();
        }
    }
}