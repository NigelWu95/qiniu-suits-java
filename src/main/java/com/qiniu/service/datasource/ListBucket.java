package com.qiniu.service.datasource;

import com.qiniu.common.QiniuException;
import com.qiniu.persistence.FileMap;
import com.qiniu.service.convert.FileInfoToMap;
import com.qiniu.service.convert.MapToString;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ListBucket implements IDataSource {

    final private Auth auth;
    final private Configuration configuration;
    final private String bucket;
    final private String marker;
    final private String end;
    final private int unitLen;
    final private List<String> prefixes;
    final private List<String> antiPrefixes;
    final private boolean prefixLeft;
    final private boolean prefixRight;
    final private String resultPath;
    private boolean saveTotal;
    private String resultFormat;
    private String resultSeparator;
    private List<String> rmFields;

    public ListBucket(Auth auth, Configuration configuration, String bucket, String marker, String end, int unitLen,
                      List<String> prefixes, List<String> antiPrefixes, boolean prefixLeft, boolean prefixRight,
                      String resultPath) {
        this.auth = auth;
        this.configuration = configuration;
        this.bucket = bucket;
        this.marker = marker;
        this.end = end;
        this.unitLen = unitLen;
        // 先设置 antiPrefixes 后再设置 prefixes，因为可能需要从 prefixes 中去除 antiPrefixes 含有的元素
        this.antiPrefixes = antiPrefixes == null ? new ArrayList<>() : antiPrefixes;
        this.prefixes = prefixes == null ? new ArrayList<>() : removeAntiPrefixes(prefixes);
        this.prefixLeft = prefixLeft;
        this.prefixRight = prefixRight;
        this.resultPath = resultPath;
        this.saveTotal = false;
    }

    public void setResultSaveOptions(boolean saveTotal, String format, String separator, List<String> removeFields) {
        this.saveTotal = saveTotal;
        this.resultFormat = format;
        this.resultSeparator = separator;
        this.rmFields = removeFields;
    }

    private List<FileLister> prefixList(List<String> prefixList, int unitLen) throws IOException {
        FileMap fileMap = new FileMap(resultPath, "list_prefix", "");
        fileMap.addErrorWriter();
        List<String> errorList = new ArrayList<>();
        List<FileLister> listerList = prefixList.parallelStream()
                .map(prefix -> {
                    FileLister fileLister = null;
                    boolean retry = true;
                    while (retry) {
                        try {
                            fileLister = new FileLister(new BucketManager(auth, configuration), bucket, prefix,
                                    marker, end, null, unitLen);
                            retry = false;
                        } catch (QiniuException e) {
                            try {
                                HttpResponseUtils.processException(e, 1, null, null);
                                System.out.println("list prefix:" + prefix + "\tretrying...");
                            } catch (IOException ex) {
                                System.out.println("list prefix:" + prefix + "\t" + e.error());
                                errorList.add(prefix + " to init fileLister" + "\t" + e.error());
                                retry = false;
                            }
                        }
                    }
                    return fileLister;
                })
                .filter(fileLister -> fileLister != null && fileLister.hasNext())
                .collect(Collectors.toList());
        if (errorList.size() > 0) fileMap.writeError(String.join("\n", errorList), false);
        fileMap.closeWriters();
        return listerList;
    }

    private List<String> removeAntiPrefixes(List<String> validPrefixList) {
        return validPrefixList.stream().filter(validPrefix -> {
            for (String antiPrefix : antiPrefixes) {
                if (validPrefix.startsWith(antiPrefix)) return false;
            }
            return true;
        }).collect(Collectors.toList());
    }

    private List<FileLister> nextLevelListBySinglePrefix(int threads, String customPrefix) throws IOException {
        List<String> originPrefixList = new ArrayList<>();
        // 由于目前指定包含 "|" 字符的前缀列举会导致超时，因此先将该字符包括 "{" 及其 ASCII 顺序之后的字符去掉（"|}~"），从而
        // 优化列举的超时问题，简化前缀参数的设置，也避免为了兼容该字符去修改代码算法
        // 去除前数个非常见作为文件名的 ASCII 字符（" !"#$%&'()*+,-"）优化前缀列举
        originPrefixList.addAll(Arrays.asList(("./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRST").split("")));
        originPrefixList.addAll(Arrays.asList(("UVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz").split("")));
        List<String> validPrefixList = new ArrayList<String>(){{add(customPrefix);}};
        List<FileLister> progressiveList = prefixList(validPrefixList, unitLen);
        List<FileLister> fileListerList = new ArrayList<>();
        // 避免重复生成新对象，将 groupedListerMap 放在循环外部
        Map<Boolean, List<FileLister>> groupedListerMap;
        int size = progressiveList.size() + 1;
        while (size > 0 && size < threads) {
            groupedListerMap = progressiveList.stream().collect(Collectors.groupingBy(FileLister::checkMarkerValid));
            if (groupedListerMap.get(false) != null) fileListerList.addAll(groupedListerMap.get(false));
            if (groupedListerMap.get(true) != null) {
                Optional<List<String>> listOptional = groupedListerMap.get(true).parallelStream().map(fileLister ->
                {
                    List<FileInfo> list = fileLister.getFileInfoList();
                    String point = "";
                    int prefixLen = fileLister.getPrefix().length();
                    if (list != null && list.size() > 0) {
                        String key0 = list.get(0).key;
                        if (key0.length() > prefixLen + 1) point = key0.substring(prefixLen, prefixLen + 1);
                        else if (key0.length() > prefixLen) point = key0.substring(prefixLen);
                    }
                    String finalPoint = point;
                    return originPrefixList.stream()
                            .filter(originPrefix -> originPrefix.compareTo(finalPoint) >= 0)
                            .map(originPrefix -> fileLister.getPrefix() + originPrefix).collect(Collectors.toList());
                }).reduce((list1, list2) -> { list1.addAll(list2); return list1; });
                if (listOptional.isPresent() && listOptional.get().size() > 0) {
                    validPrefixList = removeAntiPrefixes(listOptional.get());
                    progressiveList = prefixList(validPrefixList, unitLen);
                    size = fileListerList.size() + progressiveList.size() + 1;
                } else {
                    progressiveList = groupedListerMap.get(true);
                    break;
                }
            } else {
                progressiveList = new ArrayList<>();
                break;
            }
        }
        fileListerList.addAll(progressiveList);
        // 添加第一段 FileLister 并设置结束标志 EndKeyPrefix，及为最后一段 FileLister 修改前缀 prefix 和开始 marker
        if (fileListerList.size() > 1) {
            fileListerList.addAll(prefixList(new ArrayList<String>(){{add(customPrefix);}}, unitLen));
            fileListerList.sort(Comparator.comparing(FileLister::getPrefix));
            fileListerList.get(0).setEndKeyPrefix(fileListerList.get(1).getPrefix());
            FileLister fileLister = fileListerList.get(fileListerList.size() -1);
            fileLister.setPrefix(customPrefix);
            if (!fileLister.checkMarkerValid()) {
                FileInfo lastFileInfo = fileLister.getFileInfoList().parallelStream().filter(Objects::nonNull)
                        .max(Comparator.comparing(fileInfo -> fileInfo.key)).orElse(null);
                fileLister.setMarker(ListBucketUtils.calcMarker(lastFileInfo));
            }
        }
        return fileListerList;
    }

    private void execLister(FileLister fileLister, FileMap fileMap, ILineProcess<Map<String, String>> processor)
            throws IOException {
        ITypeConvert<FileInfo, Map<String, String>> typeConverter = new FileInfoToMap();
        ITypeConvert<Map<String, String>, String> writeTypeConverter = new MapToString(resultFormat,
                resultSeparator, rmFields);
        List<FileInfo> fileInfoList;
        List<Map<String, String>> infoMapList;
        List<String> writeList;
        while (fileLister.hasNext()) {
            fileInfoList = fileLister.next();
            while (fileLister.exception != null) {
                System.out.println("list prefix:" + fileLister.getPrefix() + " retrying...");
                HttpResponseUtils.processException(fileLister.exception, 1, fileMap, new ArrayList<String>(){{
                    add(fileLister.getPrefix() + "|" + fileLister.getMarker());
                }});
                fileLister.exception = null;
                fileInfoList = fileLister.next();
            }
            infoMapList = typeConverter.convertToVList(fileInfoList);
            if (typeConverter.getErrorList().size() > 0)
                fileMap.writeError(String.join("\n", typeConverter.consumeErrorList()), false);
            if (saveTotal) {
                writeList = writeTypeConverter.convertToVList(infoMapList);
                if (writeList.size() > 0) fileMap.writeSuccess(String.join("\n", writeList), false);
            }
            // 如果抛出异常需要检测下异常是否是可继续的异常，如果是程序可继续的异常，忽略当前异常保持数据源读取过程继续进行
            try {
                if (processor != null) processor.processLine(infoMapList);
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, 1, null, null);
            }
        }
    }

    private void export(FileMap recordFileMap, String identifier, FileLister fileLister,
                        ILineProcess<Map<String, String>> processor) throws Exception {
        FileMap fileMap = new FileMap(resultPath, "listbucket", identifier);
        fileMap.initDefaultWriters();
        ILineProcess<Map<String, String>> lineProcessor = processor == null ? null : processor.clone();
        String record = "order " + identifier + ": " + fileLister.getPrefix();
        try {
            recordFileMap.writeKeyFile("result", record + "\tlisting...", true);
            execLister(fileLister, fileMap, lineProcessor);
            if (fileLister.getMarker() == null || "".equals(fileLister.getMarker())) record += "\tsuccessfully done";
            else record += "\tmarker:" + fileLister.getMarker() + "\tend:" + fileLister.getEndKeyPrefix();
            System.out.println(record);
        } catch (QiniuException e) {
            record += "\tmarker:" + fileLister.getMarker() + "\tend:" + fileLister.getEndKeyPrefix() +
                    "\t" + e.getMessage().replaceAll("\n", "\t");
            throw e;
        } finally {
            recordFileMap.writeKeyFile("result", record, true);
            fileMap.closeWriters();
            if (lineProcessor != null) lineProcessor.closeResource();
            fileLister.remove();
        }
    }

    synchronized private void exit(AtomicBoolean exit, Exception e) {
        if (!exit.get()) e.printStackTrace();
        exit.set(true);
        System.exit(-1);
    }

    private void execInThreads(ExecutorService executorPool, AtomicBoolean exit, List<FileLister> fileListerList,
                               FileMap recordFileMap, int alreadyOrder, ILineProcess<Map<String, String>> processor) {
        for (int j = 0; j < fileListerList.size(); j++) {
            int finalJ = j;
            FileLister lister = fileListerList.get(finalJ);
            executorPool.execute(() -> {
                try {
                    export(recordFileMap, String.valueOf(finalJ + 1 + alreadyOrder), lister, processor);
                } catch (Exception e) {
                    exit(exit, e);
                }
            });
        }
    }

    public void export(int threads, ILineProcess<Map<String, String>> processor) throws Exception {
        String info = "list bucket" + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " running...");
        FileMap recordFileMap = new FileMap(resultPath);
        ExecutorService executorPool = Executors.newFixedThreadPool(threads);
        AtomicBoolean exit = new AtomicBoolean(false);
        Collections.sort(prefixes);
        int alreadyOrder = 0;
        List<FileLister> fileListerList = new ArrayList<>();
        if (prefixes.size() == 0) {
            fileListerList = nextLevelListBySinglePrefix(threads, "");
            execInThreads(executorPool, exit, fileListerList, recordFileMap, alreadyOrder, processor);
        } else {
            for (int i = 0; i < prefixes.size(); i++) {
                fileListerList.addAll(nextLevelListBySinglePrefix(threads, prefixes.get(i)));
                if (i == 0) {
                    if (prefixLeft) {
                        List<FileLister> firstLister = prefixList(new ArrayList<String>(){{add("");}}, unitLen);
                        firstLister.get(0).setEndKeyPrefix(prefixes.get(0));
                        fileListerList.addAll(firstLister);
                    }
                } else if (i == prefixes.size() - 1) {
                    // 为第一段 FileLister 设置结束标志 EndKeyPrefix，及为最后一段 FileLister 修改前缀 prefix 和开始 marker
                    if (fileListerList.size() > 1) {
                        fileListerList.sort(Comparator.comparing(FileLister::getPrefix));
                        if (prefixRight) fileListerList.get(fileListerList.size() -1).setPrefix("");
                    }
                }
                if (fileListerList.size() >= threads) {
                    execInThreads(executorPool, exit, fileListerList, recordFileMap, alreadyOrder, processor);
                    alreadyOrder += fileListerList.size();
                    fileListerList = new ArrayList<>();
                }
            }
            execInThreads(executorPool, exit, fileListerList, recordFileMap, alreadyOrder, processor);
        }
        executorPool.shutdown();
        recordFileMap.writeKeyFile("count_" + (alreadyOrder + fileListerList.size()), null, false);
        while (!executorPool.isTerminated()) Thread.sleep(1000);
        recordFileMap.closeWriters();
        System.out.println(info + " finished");
    }
}
