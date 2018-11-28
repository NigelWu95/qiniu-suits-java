package com.qiniu.service.datasource;

import com.google.gson.JsonObject;
import com.qiniu.common.FileMap;
import com.qiniu.common.ListFileAntiFilter;
import com.qiniu.common.ListFileFilter;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager;
import com.qiniu.service.interfaces.ILineProcess;
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
    private String cPrefix;
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
        this.cPrefix = customPrefix == null ? "" : customPrefix;
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
                        FileLister fileLister = null;
                        try {
                            fileLister = new FileLister(new BucketManager(auth, configuration), bucket, prefix,
                                    null, null, unitLen, version, retryCount);
                        } catch (QiniuException e1) {
                            HttpResponseUtils.checkRetryCount(e1, retryCount);
                            while (retryCount > 0) {
                                System.out.println("list prefix:" + prefix + "\tretrying...");
                                try {
                                    fileLister = new FileLister(new BucketManager(auth, configuration), bucket, prefix,
                                            null, null, unitLen, version, retryCount);
                                    retryCount = 0;
                                } catch (QiniuException e2) {
                                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                                }
                            }
                        }
                        return fileLister;
                    } catch (QiniuException e) {
                        System.out.println("list prefix:" + prefix + "\t" + e.error());
                        return null;
//                        throw new RuntimeException("list prefix:" + prefix + "\t" + e.error(), e.fillInStackTrace());
                    }
                })
                .filter(Objects::nonNull)
                .filter(FileLister::hasNext)
                .collect(Collectors.toList());
    }

    private List<FileLister> getFileListerList(int unitLen, int level) throws QiniuException {
        List<String> validPrefixList = originPrefixList.parallelStream()
                .filter(originPrefix -> !antiPrefix.contains(originPrefix))
                .map(prefix -> cPrefix + prefix)
                .collect(Collectors.toList());
        List<FileLister> fileListerList = new ArrayList<>();
        FileLister firstFileLister = new FileLister(new BucketManager(auth, configuration), bucket, cPrefix,
                null, null, unitLen, version, retryCount);

        if (level == 1) {
//            validPrefixList.add(cPrefix);
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
//            level2PrefixList.add(cPrefix);
            fileListerList = prefixList(level2PrefixList, unitLen);
        }
        fileListerList.add(firstFileLister);

        return fileListerList;
    }

    private void recordProgress(String prefix, String marker, String endFile, FileMap fileMap) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("prefix", prefix);
        jsonObject.addProperty("marker", marker);
        jsonObject.addProperty("end", endFile);
        fileMap.writeKeyFile("marker" + fileMap.getSuffix(), JsonConvertUtils.toJsonWithoutUrlEscape(jsonObject));
    }

    private void listFromLister(FileLister fileLister, String endFile, int resultIndex, ILineProcess processor) {

        FileMap fileMap = new FileMap();
        ILineProcess fileProcessor = null;
        try {
            fileProcessor = processor != null ? processor.getNewInstance(resultIndex) : null;
            fileMap.initWriter(resultFileDir, "list", resultIndex);

            while (fileLister.hasNext()) {
                String marker = fileLister.getMarker();
                List<FileInfo> fileInfoList = fileLister.next();
                int maxError = 20 * retryCount;
                while (fileLister.exception != null) {
                    maxError--;
                    if (maxError <= 0) HttpResponseUtils.processException(fileLister.exception, fileMap, "list",
                            fileLister.getPrefix() + "|" + marker);
                    System.out.println("list prefix:" + fileLister.getPrefix() + "|end:" + endFile + "\t" +
                            fileLister.error() + " retrying...");
                    fileLister.exception = null;
                    fileInfoList = fileLister.next();
                }
                if (!StringUtils.isNullOrEmpty(endFile)) {
                    fileInfoList = fileInfoList.parallelStream()
                            .filter(fileInfo -> fileInfo.key.compareTo(endFile) < 0)
                            .collect(Collectors.toList());
                }
                writeResult(fileInfoList, fileMap, 1);
                if (doFilter || doAntiFilter) {
                    fileInfoList = filterFileInfo(fileInfoList);
                    writeResult(fileInfoList, fileMap, 2);
                }
                if (fileProcessor != null) fileProcessor.processLine(fileInfoList.parallelStream()
                        .filter(Objects::nonNull).collect(Collectors.toList()));
                recordProgress(fileLister.getPrefix(), marker, endFile, fileMap);
                if (!StringUtils.isNullOrEmpty(endFile)) {
                    if (fileInfoList.parallelStream().anyMatch(fileInfo -> endFile.compareTo(fileInfo.key) <= 0))
                        break;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            fileMap.closeWriter();
            if (fileProcessor != null) fileProcessor.closeResource();
            fileLister.remove();
            fileLister = null;
        }
    }

    public void concurrentlyList(int maxThreads, int level, ILineProcess processor) throws QiniuException {
        List<FileLister> fileListerList = getFileListerList(unitLen, level);
        fileListerList.sort(Comparator.comparing(FileLister::getPrefix));
        String firstEnd = "";
        if (fileListerList.size() > 1) {
            firstEnd = fileListerList.get(1).getPrefix();
            FileLister fileLister = fileListerList.get(fileListerList.size() -1);
            fileLister.setPrefix(cPrefix);
            if (StringUtils.isNullOrEmpty(fileLister.getMarker())) {
                FileInfo lastFileInfo = fileLister.getFileInfoList().parallelStream()
                        .filter(Objects::nonNull)
                        .max(Comparator.comparing(fileInfo -> fileInfo.key))
                        .orElse(null);
                String marker = ListBucketUtils.calcMarker(lastFileInfo);
                fileLister.setMarker(marker);
            }
        }
        int listSize = fileListerList.size();
        int runningThreads = listSize < maxThreads ? listSize : maxThreads;
        String info = "list bucket" + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " concurrently running with " + runningThreads + " threads ...");
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable);
            thread.setUncaughtExceptionHandler((t, e) -> System.out.println(t.getName() + "\t" + e.getMessage()));
            return thread;
        };
        ExecutorService executorPool = Executors.newFixedThreadPool(runningThreads, threadFactory);
        for (int i = 0; i < fileListerList.size(); i++) {
            int finalI = i;
            String finalEnd = i == 0 ? firstEnd : "";
            executorPool.execute(() -> listFromLister(fileListerList.get(finalI), finalEnd, finalI + 1, processor));
        }
        executorPool.shutdown();
        ExecutorsUtils.waitForShutdown(executorPool, info);
        if (processor != null) processor.closeResource();
    }

    public void checkValidPrefix(int level) throws IOException {
        List<FileLister> fileListerList = getFileListerList(1, level);
        FileMap fileMap = new FileMap();
        try {
            fileMap.initWriter(resultFileDir, "list", "check");
            List<String> validPrefixAndMarker = fileListerList.parallelStream()
                    .filter(FileLister::hasNext)
                    .map(fileLister -> fileLister.getPrefix() + "\t" + fileLister.getMarker())
                    .collect(Collectors.toList());
            fileMap.writeSuccess(String.join("\n", validPrefixAndMarker));
        } finally {
            fileMap.closeWriter();
        }
    }

    public void straightlyList(String marker, String end, ILineProcess processor) throws IOException {
        String info = "list bucket" + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " start...");
        BucketManager bucketManager = new BucketManager(auth, configuration);
        FileLister fileLister = new FileLister(bucketManager, bucket, cPrefix, "", marker, unitLen,
                version, retryCount);
        listFromLister(fileLister, end, 0, processor);
        System.out.println(info + " finished.");
        if (processor != null) processor.closeResource();
    }
}
