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
    private String cPrefix;
    private List<String> antiPrefix;
    private int retryCount;
    private List<String> originPrefixList = Arrays.asList((" !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRST" +
            "UVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~").split(""));
    private String resultPath;
    private boolean saveTotal;
    private String resultFormat;
    private String resultSeparator;
    private List<String> resultFields;

    public ListBucket(Auth auth, Configuration configuration, String bucket, int unitLen, String customPrefix,
                      List<String> antiPrefix, int retryCount, String resultPath) {
        this.auth = auth;
        this.configuration = configuration;
        this.bucket = bucket;
        this.unitLen = unitLen;
        this.cPrefix = customPrefix == null ? "" : customPrefix;
        this.antiPrefix = antiPrefix == null ? new ArrayList<>() : antiPrefix;
        this.retryCount = retryCount;
        this.resultPath = resultPath;
        this.saveTotal = false;
    }

    public void setResultSaveOptions(String format, String separator, List<String> fields) {
        this.saveTotal = true;
        this.resultFormat = format;
        this.resultSeparator = separator;
        this.resultFields = fields;
    }

    private List<FileLister> prefixList(List<String> prefixList, int unitLen) {

        return prefixList.parallelStream()
                .map(prefix -> {
                    try {
                        FileLister fileLister = null;
                        try {
                            fileLister = new FileLister(new BucketManager(auth, configuration), bucket, prefix,
                                    null, "", null, unitLen);
                        } catch (QiniuException e1) {
                            HttpResponseUtils.checkRetryCount(e1, retryCount);
                            while (retryCount > 0) {
                                System.out.println("list prefix:" + prefix + "\tretrying...");
                                try {
                                    fileLister = new FileLister(new BucketManager(auth, configuration), bucket, prefix,
                                            null, "", null, unitLen);
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
//                .filter(Objects::nonNull)
//                .filter(FileLister::hasNext)
                .filter(fileLister -> {
                    if (fileLister != null && fileLister.hasNext()) return true;
                    else {
                        fileLister = null;
                        return false;
                    }
                })
                .collect(Collectors.toList());
    }

    private List<FileLister> getFileListerList(int threads) {
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
                            .filter(prefix -> prefix.compareTo(finalPoint) >= 0)
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

    private void execLister(FileLister fileLister, FileMap fileMap, ILineProcess processor) throws Exception {
        ITypeConvert<FileInfo, Map<String, String>> typeConverter = new FileInfoToMap();
        ITypeConvert<FileInfo, String> writeTypeConverter = null;
        if (saveTotal) {
            writeTypeConverter = new FileInfoToString(resultFormat, resultSeparator, resultFields);
            fileMap.initDefaultWriters();
        }
        String marker;
        List<FileInfo> fileInfoList;
        List<String> writeList;
        while (fileLister.hasNext()) {
            marker = fileLister.getMarker();
            fileInfoList = fileLister.next();
            int maxError = 20 * retryCount;
            while (fileLister.exception != null) {
                System.out.println("list prefix:" + fileLister.getPrefix() + " retrying...");
                maxError--;
                if (maxError <= 0) HttpResponseUtils.processException(fileLister.exception, fileMap,
                        fileLister.getPrefix() + "|" + marker);
                fileLister.exception = null;
                fileInfoList = fileLister.next();
            }
            if (saveTotal) {
                writeList = writeTypeConverter.convertToVList(fileInfoList);
                if (writeList.size() > 0) fileMap.writeSuccess(String.join("\n", writeList));
                if (writeTypeConverter.getErrorList().size() > 0)
                    fileMap.writeError(String.join("\n", writeTypeConverter.getErrorList()));
            }
            if (processor != null) processor.processLine(typeConverter.convertToVList(fileInfoList));
            if (typeConverter.getErrorList().size() > 0)
                fileMap.writeKeyFile("to_map", String.join("\n", typeConverter.getErrorList()));
        }
    }

    public void concurrentlyList(int threads, ILineProcess<Map<String, String>> processor) throws Exception {
        List<FileLister> fileListerList = getFileListerList(threads);
        String info = "list bucket" + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " concurrently running with " + threads + " threads ...");
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable);
            thread.setUncaughtExceptionHandler((t, e) -> {
                System.out.println(t.getName() + "\t" + e.getMessage());
                e.printStackTrace();
            });
            return thread;
        };
        ExecutorService executorPool = Executors.newFixedThreadPool(threads, threadFactory);
        List<String> prefixList = new ArrayList<>();
        for (int i = 0; i < fileListerList.size(); i++) {
            final int finalI = i;
            ILineProcess lineProcessor = processor == null ? null : i == 0 ? processor : processor.clone();
            executorPool.execute(() -> {
                FileLister fileLister = fileListerList.get(finalI);
                FileMap fileMap = new FileMap(resultPath, "listbucket", String.valueOf(finalI));
                if (lineProcessor != null) lineProcessor.setResultTag(fileLister.getPrefix());
                try {
                    execLister(fileLister, fileMap, lineProcessor);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    String marker = fileLister.getMarker();
                    if (marker != null && !"".equals(marker)) prefixList.add(fileLister.getPrefix() + "\tdone.");
                    else prefixList.add(fileLister.getPrefix() + "\t" + marker + "\t" + fileLister.getEndKeyPrefix());
                    fileMap.closeWriter();
                    if (processor != null) processor.closeResource();
                    fileLister.remove();
                    fileLister = null;
                }
            });
        }
        executorPool.shutdown();
        ExecutorsUtils.waitForShutdown(executorPool, info);
        FileMap fileMap = new FileMap(resultPath);
        fileMap.writeKeyFile("list_prefix", String.join("\n", prefixList));
        if (processor != null) processor.closeResource();
    }

    public void straightlyList(String marker, String end, ILineProcess<Map<String, String>> processor) throws Exception {
        String info = "list bucket" + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " start...");
        BucketManager bucketManager = new BucketManager(auth, configuration);
        FileLister fileLister = new FileLister(bucketManager, bucket, cPrefix, marker, end, "", unitLen);
        FileMap fileMap = new FileMap(resultPath, "listbucket", fileLister.getPrefix());
        execLister(fileLister, fileMap, processor);
        System.out.println(info + " finished.");
        if (processor != null) processor.closeResource();
    }
}
