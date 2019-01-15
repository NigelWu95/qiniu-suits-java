package com.qiniu.service.datasource;

import com.qiniu.common.QiniuException;
import com.qiniu.persistence.FileMap;
import com.qiniu.service.convert.FileInfoToMap;
import com.qiniu.service.convert.FileInfoToString;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.service.qoss.FileLister;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

public class ListBucket implements IDataSource {

    final private Auth auth;
    final private Configuration configuration;
    final private String bucket;
    final private int unitLen;
    final private String cPrefix;
    final private List<String> antiPrefix;
    private int retryCount;
    final private String resultPath;
    private boolean saveTotal;
    private String resultFormat;
    private String resultSeparator;
    private List<String> rmFields;

    public ListBucket(Auth auth, Configuration configuration, String bucket, int unitLen, String customPrefix,
                      List<String> antiPrefix, String resultPath) {
        this.auth = auth;
        this.configuration = configuration;
        this.bucket = bucket;
        this.unitLen = unitLen;
        this.cPrefix = customPrefix == null ? "" : customPrefix;
        this.antiPrefix = antiPrefix == null ? new ArrayList<>() : antiPrefix;
        this.retryCount = 3;
        this.resultPath = resultPath;
        this.saveTotal = false;
    }

    public void setResultSaveOptions(String format, String separator, List<String> removeFields) {
        this.saveTotal = true;
        this.resultFormat = format;
        this.resultSeparator = separator;
        this.rmFields = removeFields;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount < 1 ? 1 : retryCount;
    }

    private List<FileLister> prefixList(List<String> prefixList, int unitLen) {
        FileMap fileMap = new FileMap(resultPath);
        return prefixList.parallelStream()
                .map(prefix -> {
                    try {
                        FileLister fileLister = null;
                        int retry = retryCount;
                        try {
                            fileLister = new FileLister(new BucketManager(auth, configuration), bucket, prefix,
                                    null, "", null, unitLen);
                        } catch (QiniuException e1) {
                            HttpResponseUtils.checkRetryCount(e1, retry);
                            while (retry > 0) {
                                System.out.println("list prefix:" + prefix + "\tlast " + retry + " times retrying...");
                                try {
                                    fileLister = new FileLister(new BucketManager(auth, configuration), bucket, prefix,
                                            null, "", null, unitLen);
                                    retry = 0;
                                } catch (QiniuException e2) {
                                    retry = HttpResponseUtils.getNextRetryCount(e2, retry);
                                }
                            }
                        }
                        return fileLister;
                    } catch (QiniuException e) {
                        System.out.println("list prefix:" + prefix + "\t" + e.error());
                        try {
                            fileMap.writeKeyFile("prefix_error", prefix + " to init fileLister" +
                                    "\t" + e.error());
                        } catch (IOException e1) {
                            e1.printStackTrace();
                        }
                        return null;
                    }
                })
                .filter(fileLister -> fileLister != null && fileLister.hasNext())
                .collect(Collectors.toList());
    }

    private List<FileLister> getFileListerList(int threads) {
        List<String> originPrefixList = Arrays.asList((" !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRST" +
                "UVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~").split(""));
        if (threads <= 1) return prefixList(new ArrayList<String>(){{add(cPrefix);}}, unitLen);
        List<String> validPrefixList = originPrefixList.parallelStream().filter(originPrefix ->
                !antiPrefix.contains(originPrefix)).map(prefix -> cPrefix + prefix).collect(Collectors.toList());
        List<FileLister> fileListerList = prefixList(validPrefixList, unitLen);
        Map<Boolean, List<FileLister>> groupedFileListerMap;
        while (fileListerList.size() < threads - 1 && fileListerList.size() > 0) {
            groupedFileListerMap = fileListerList.stream().collect(Collectors.groupingBy(fileLister ->
                    fileLister.getMarker() != null && !"".equals(fileLister.getMarker())));
            if (groupedFileListerMap.get(true) != null) {
                Optional<List<String>> listOptional = groupedFileListerMap.get(true).parallelStream().map(fileLister ->
                {
                    List<FileInfo> list = fileLister.getFileInfoList();
                    String point = "";
                    if (list != null && list.size() > 0) {
                        int prefixLen = fileLister.getPrefix().length();
                        point = list.get(0).key.substring(prefixLen, prefixLen + 1);
                    }
                    String finalPoint = point;
                    return originPrefixList.stream()
                            .filter(prefix -> prefix.compareTo(finalPoint) >= 0 && !antiPrefix.contains(prefix))
                            .map(originPrefix -> fileLister.getPrefix() + originPrefix).collect(Collectors.toList());
                }).reduce((list1, list2) -> { list1.addAll(list2); return list1; });
                if (listOptional.isPresent()) {
                    validPrefixList = listOptional.get().parallelStream()
                            .filter(originPrefix -> !antiPrefix.contains(originPrefix)).collect(Collectors.toList());
                    fileListerList = prefixList(validPrefixList, unitLen);
                }
            } else {
                break;
            }
            if (groupedFileListerMap.get(false) != null) fileListerList.addAll(groupedFileListerMap.get(false));
        }
        // 加入第一段 FileLister，第一段 Lister 使用的 prefix 为 cPrefix（空或者传入的参数）
        fileListerList.addAll(prefixList(new ArrayList<String>(){{add(cPrefix);}}, unitLen));
        if (fileListerList.size() > 1) {
            fileListerList.sort(Comparator.comparing(FileLister::getPrefix));
            fileListerList.get(0).setEndKeyPrefix(fileListerList.get(1).getPrefix());
            FileLister fileLister = fileListerList.get(fileListerList.size() -1);
            fileLister.setPrefix(cPrefix);
            if (fileLister.getMarker() == null || "".equals(fileLister.getMarker())) {
                FileInfo lastFileInfo = fileLister.getFileInfoList().parallelStream().filter(Objects::nonNull)
                        .max(Comparator.comparing(fileInfo -> fileInfo.key)).orElse(null);
                fileLister.setMarker(ListBucketUtils.calcMarker(lastFileInfo));
            }
        }

        return fileListerList;
    }

    private void execLister(FileLister fileLister, FileMap fileMap, ILineProcess processor) throws QiniuException {
        ITypeConvert<FileInfo, Map<String, String>> typeConverter = new FileInfoToMap();
        ITypeConvert<FileInfo, String> writeTypeConverter = new FileInfoToString(resultFormat, resultSeparator, rmFields);
        List<FileInfo> fileInfoList;
        List<String> writeList;
        while (fileLister.hasNext()) {
            fileInfoList = fileLister.next();
            while (fileLister.exception != null) {
                System.out.println("list prefix:" + fileLister.getPrefix() + " retrying...");
                HttpResponseUtils.processException(fileLister.exception, fileMap, new ArrayList<String>(){{
                    add(fileLister.getPrefix() + "|" + fileLister.getMarker());
                }});
                fileLister.exception = null;
                fileInfoList = fileLister.next();
            }
            if (saveTotal) {
                writeList = writeTypeConverter.convertToVList(fileInfoList);
                if (writeList.size() > 0) fileMap.writeSuccess(String.join("\n", writeList));
                if (writeTypeConverter.getErrorList().size() > 0)
                    fileMap.writeError(String.join("\n", writeTypeConverter.consumeErrorList()));
            }
            if (processor != null) processor.processLine(typeConverter.convertToVList(fileInfoList));
            if (typeConverter.getErrorList().size() > 0)
                fileMap.writeError(String.join("\n", typeConverter.consumeErrorList()));
        }
    }

    public void exportData(int threads, ILineProcess<Map<String, String>> processor) throws Exception {
        List<FileLister> fileListerList = getFileListerList(threads);
        String info = "list bucket" + (processor == null ? "" : " and " + processor.getProcessName());
        FileMap recordFileMap = new FileMap(resultPath);
        System.out.println(info + " concurrently running with " + threads + " threads ...");
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable);
            thread.setUncaughtExceptionHandler((t, e) -> {
                System.out.println(t.getName() + "\t" + t.toString());
                recordFileMap.closeWriters();
                System.exit(-1);
            });
            return thread;
        };
        ExecutorService executorPool = Executors.newFixedThreadPool(threads, threadFactory);
        for (int i = 0; i < fileListerList.size(); i++) {
            FileLister fileLister = fileListerList.get(i);
            FileMap fileMap = new FileMap(resultPath, "listbucket", String.valueOf(i + 1));
            fileMap.initDefaultWriters();
            ILineProcess lineProcessor = processor == null ? null : processor.clone();
            int finalI = i;
            executorPool.execute(() -> {
                String record = "order " + String.valueOf(finalI + 1) + ": " + fileLister.getPrefix();
                try {
                    execLister(fileLister, fileMap, lineProcessor);
                    if (fileLister.getMarker() == null || "".equals(fileLister.getMarker()))
                        record += "\tsuccessfully done";
                    else
                        record += "\tmarker:" + fileLister.getMarker() + "\tend:" + fileLister.getEndKeyPrefix();
                    System.out.println(record);
                } catch (QiniuException e) {
                    System.out.println(record + "\tmarker:" + fileLister.getMarker());
                    record += "\tmarker:" + fileLister.getMarker() + "\tend:" + fileLister.getEndKeyPrefix() +
                            "\t" + e.getMessage();
                    e.printStackTrace();
                    throw new RuntimeException(e);
                } finally {
                    try {
                        recordFileMap.writeKeyFile("result", record.replaceAll("\n", "\t"));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    fileMap.closeWriters();
                    if (lineProcessor != null) lineProcessor.closeResource();
                    fileLister.remove();
                }
            });
        }
        executorPool.shutdown();
        ExecutorsUtils.waitForShutdown(executorPool, info);
        recordFileMap.closeWriters();
    }
}
