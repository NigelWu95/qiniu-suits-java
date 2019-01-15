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
import java.util.Map.*;
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
        this.resultPath = resultPath;
        this.saveTotal = false;
    }

    public void setResultSaveOptions(String format, String separator, List<String> removeFields) {
        this.saveTotal = true;
        this.resultFormat = format;
        this.resultSeparator = separator;
        this.rmFields = removeFields;
    }

    private List<FileLister> prefixList(List<String> prefixList, int unitLen) throws IOException {
        FileMap fileMap = new FileMap(resultPath, "list_prefix", "");
        fileMap.initDefaultWriters();
        List<FileLister> listerList = prefixList.parallelStream()
                .map(prefix -> {
                    try {
                        FileLister fileLister = null;
                        boolean retry = true;
                        while (retry) {
                            try {
                                fileLister = new FileLister(new BucketManager(auth, configuration), bucket, prefix,
                                        null, "", null, unitLen);
                                retry = false;
                            } catch (QiniuException e) {
                                System.out.println("list prefix:" + prefix + "\tretrying...");
                                HttpResponseUtils.checkRetryCount(e, 1);
                            }
                        }
                        return fileLister;
                    } catch (QiniuException e) {
                        System.out.println("list prefix:" + prefix + "\t" + e.error());
                        fileMap.writeSuccess(prefix + " to init fileLister" + "\t" + e.error());
                        return null;
                    }
                })
                .filter(fileLister -> fileLister != null && fileLister.hasNext())
                .collect(Collectors.toList());
        fileMap.closeWriters();
        return listerList;
    }

    private List<FileLister> getFileListerList(int threads) throws IOException {
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
            // 如果抛出异常需要检测下异常是否是可继续的异常，如果是程序可继续的异常，忽略当前异常保持数据源读取过程继续进行
            try {
                if (processor != null) processor.processLine(typeConverter.convertToVList(fileInfoList));
            } catch (QiniuException e) {
                HttpResponseUtils.checkRetryCount(e, 1);
            }
            if (typeConverter.getErrorList().size() > 0)
                fileMap.writeError(String.join("\n", typeConverter.consumeErrorList()));
        }
    }

    public void export(Entry<String, FileLister> fileListerMap, ILineProcess<Map<String, String>> processor)
            throws Exception {
        FileMap recordFileMap = new FileMap(resultPath);
        FileLister fileLister = fileListerMap.getValue();
        FileMap fileMap = new FileMap(resultPath, "listbucket", fileListerMap.getKey());
        fileMap.initDefaultWriters();
        ILineProcess lineProcessor = processor == null ? null : processor.clone();
        String record = "order " + fileListerMap.getKey() + ": " + fileLister.getPrefix();
        String exception = "";
        try {
            execLister(fileListerMap.getValue(), fileMap, lineProcessor);
        } catch (QiniuException e) {
            exception = e.getMessage();
            e.printStackTrace();
            throw e;
        } finally {
            try {
                String next = fileLister.getMarker();
                if (next == null || "".equals(next)) record += "\tsuccessfully done";
                else record += "\tmarker:" + next + "\tend:" + fileLister.getEndKeyPrefix() + "\t" + exception;
                System.out.println(record);
                recordFileMap.writeKeyFile("result", record.replaceAll("\n", "\t"));
            } catch (IOException e) {
                e.printStackTrace();
            }
            fileMap.closeWriters();
            recordFileMap.closeWriters();
            if (lineProcessor != null) lineProcessor.closeResource();
            fileLister.remove();
        }
    }

    public void export(int threads, ILineProcess<Map<String, String>> processor) throws Exception {
        List<FileLister> fileListerList = getFileListerList(threads);
        Map<String, FileLister> fileListerMap = new HashMap<String, FileLister>(){{
            for (int i = 0; i < fileListerList.size(); i++) {
                put(String.valueOf(i + 1), fileListerList.get(i));
            }
        }};
        String info = "list bucket" + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " concurrently running with " + threads + " threads ...");
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable);
            thread.setUncaughtExceptionHandler((t, e) -> {
                System.out.println(t.getName() + "\t" + t.toString());
                System.exit(-1);
            });
            return thread;
        };
        ExecutorService executorPool = Executors.newFixedThreadPool(threads, threadFactory);
        for (Entry<String, FileLister> fileListerEntry : fileListerMap.entrySet()) {
            executorPool.execute(() -> {
                try {
                    export(fileListerEntry, processor);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        executorPool.shutdown();
        ExecutorsUtils.waitForShutdown(executorPool, info);
    }
}
