package com.qiniu.service.datasource;

import com.qiniu.common.QiniuException;
import com.qiniu.sdk.BucketManager;
import com.qiniu.service.help.ProgressRecorder;
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

public class ListBucket {

    private Auth auth;
    private Configuration configuration;
    private String bucket;
    private int unitLen;
    private int version;
    private String cPrefix;
    private List<String> antiPrefix;
    private int retryCount;
    private List<String> originPrefixList = Arrays.asList(
            " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
                    .split(""));
    private ProgressRecorder progressRecorder;

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

    public void setProgressRecorder(ProgressRecorder progressRecorder) {
        this.progressRecorder = progressRecorder;
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

    private void listFromLister(FileLister fileLister, String endFile, int resultIndex, ILineProcess<FileInfo> processor) {

        ILineProcess<FileInfo> fileProcessor = null;
        try {
            fileProcessor = processor != null ? processor.getNewInstance(resultIndex) : null;
            ProgressRecorder recorder = progressRecorder != null ? progressRecorder.getNewInstance(resultIndex) : null;

            while (fileLister.hasNext()) {
                String marker = fileLister.getMarker();
                List<FileInfo> fileInfoList = fileLister.next();
                int maxError = 20 * retryCount;
                while (fileLister.exception != null) {
                    System.out.println("list prefix:" + fileLister.getPrefix() + "|end:" + endFile + "\t" +
                            fileLister.error() + " retrying...");
                    maxError--;
                    if (maxError <= 0) HttpResponseUtils.processException(fileLister.exception, null, "list",
                            fileLister.getPrefix() + "|" + marker);
                    fileLister.exception = null;
                    fileInfoList = fileLister.next();
                }
                int size = fileInfoList.size();
                int finaSize = size;
                if (!StringUtils.isNullOrEmpty(endFile)) {
                    fileInfoList = fileInfoList.parallelStream()
                            .filter(fileInfo -> fileInfo.key.compareTo(endFile) < 0)
                            .collect(Collectors.toList());
                    finaSize = fileInfoList.size();
                }
                if (fileProcessor != null) fileProcessor.processLine(fileInfoList);
                if (recorder != null) recorder.record(fileLister.getPrefix(), marker, endFile);
                if (finaSize < size) break;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (fileProcessor != null) fileProcessor.closeResource();
            fileLister.remove();
            fileLister = null;
        }
    }

    public void concurrentlyList(int maxThreads, int level, ILineProcess<FileInfo> processor) throws QiniuException {
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
            thread.setUncaughtExceptionHandler((t, e) -> {
                System.out.println(t.getName() + "\t" + e.getMessage());
                e.printStackTrace();
            });
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

    public void straightlyList(String marker, String end, ILineProcess<FileInfo> processor) throws IOException {
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
