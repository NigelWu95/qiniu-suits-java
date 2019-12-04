package com.qiniu.datasource;

import com.qiniu.common.SuitsException;
import com.qiniu.convert.Converter;
import com.qiniu.convert.JsonObjectPair;
import com.qiniu.convert.StringBuilderPair;
import com.qiniu.convert.StringMapPair;
import com.qiniu.interfaces.IStorageLister;
import com.qiniu.interfaces.IStringFormat;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.interfaces.IResultOutput;
import com.qiniu.sdk.FileItem;
import com.qiniu.sdk.UpYunClient;
import com.qiniu.sdk.UpYunConfig;
import com.qiniu.util.CloudApiUtils;
import com.qiniu.util.ConvertingUtils;
import com.qiniu.util.FileUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class UpYosContainer extends CloudStorageContainer<FileItem, BufferedWriter, Map<String, String>> {

    private String username;
    private String password;
    private UpYunConfig configuration;

    public UpYosContainer(String username, String password, UpYunConfig configuration, String bucket,
                          Map<String, Map<String, String>> prefixesMap, List<String> antiPrefixes,
//                             boolean prefixLeft, boolean prefixRight,
                          Map<String, String> indexMap, List<String> fields, int unitLen, int threads) throws IOException {
        super(bucket, prefixesMap, antiPrefixes, false, false, indexMap, fields, unitLen, threads);
        this.username = username;
        this.password = password;
        this.configuration = configuration;
        UpLister upLister = new UpLister(new UpYunClient(configuration, username, password), bucket, null,
                null, null, 1);
        upLister.close();
        upLister = null;
        FileItem test = new FileItem();
        test.key = "test";
        ConvertingUtils.toPair(test, indexMap, new StringMapPair());
    }

    @Override
    public String getSourceName() {
        return "upyun";
    }

    @Override
    protected ITypeConvert<FileItem, Map<String, String>> getNewConverter() {
        return new Converter<FileItem, Map<String, String>>() {
            @Override
            public Map<String, String> convertToV(FileItem line) throws IOException {
                return ConvertingUtils.toPair(line, indexMap, new StringMapPair());
            }
        };
    }

    @Override
    protected ITypeConvert<FileItem, String> getNewStringConverter() {
        IStringFormat<FileItem> stringFormatter;
        if ("json".equals(saveFormat)) {
            stringFormatter = line -> ConvertingUtils.toPair(line, fields, new JsonObjectPair()).toString();
        } else if ("yaml".equals(saveFormat)) {
            stringFormatter = line -> ConvertingUtils.toStringWithIndent(line, fields);
        } else {
            stringFormatter = line -> ConvertingUtils.toPair(line, fields, new StringBuilderPair(saveSeparator));
        }
        return new Converter<FileItem, String>() {
            @Override
            public String convertToV(FileItem line) throws IOException {
                return stringFormatter.toFormatString(line);
            }
        };
    }

    @Override
    protected IResultOutput<BufferedWriter> getNewResultSaver(String order) throws IOException {
        return order != null ? new FileSaveMapper(savePath, getSourceName(), order) : new FileSaveMapper(savePath);
    }

    @Override
    protected IStorageLister<FileItem> getLister(String prefix, String marker, String start, String end, int unitLen) throws SuitsException {
        if (marker == null || "".equals(marker)) marker = CloudApiUtils.getUpYunMarker(username, password, bucket, start);
        return new UpLister(new UpYunClient(configuration, username, password), bucket, prefix, marker, end, unitLen);
    }

    private List<String> directoriesAfterListerRun(String prefix) {
        try {
            IStorageLister<FileItem> lister = generateLister(prefix);
            if (lister.hasNext() || lister.getDirectories() != null) {
                listing(lister);
                if (lister.getDirectories() == null || lister.getDirectories().size() <= 0) {
                    return null;
                } else if (hasAntiPrefixes) {
                    return lister.getDirectories().stream().filter(this::checkPrefix)
                            .peek(this::recordListerByPrefix).collect(Collectors.toList());
                } else {
                    for (String dir : lister.getDirectories()) recordListerByPrefix(dir);
                    return lister.getDirectories();
                }
            } else {
                listing(lister);
                return lister.getDirectories();
            }
        } catch (SuitsException e) {
            try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
            errorLogger.error("generate lister failed by {}\t{}", prefix, prefixesMap.get(prefix), e);
            return null;
        }
    }

    private AtomicLong atomicLong = new AtomicLong(0);

    private void listForNextIteratively(List<String> prefixes) throws Exception {
        List<String> tempPrefixes;
        List<Future<List<String>>> futures = new ArrayList<>();
        for (String prefix : prefixes) {
            if (atomicLong.get() > threads) {
                tempPrefixes = directoriesAfterListerRun(prefix);
                if (tempPrefixes != null) listForNextIteratively(tempPrefixes);
            } else {
                atomicLong.incrementAndGet();
                futures.add(executorPool.submit(() -> {
                    List<String> list = directoriesAfterListerRun(prefix);
                    atomicLong.decrementAndGet();
                    return list;
                }));
            }
        }
        Iterator<Future<List<String>>> iterator;
        Future<List<String>> future;
        while (futures.size() > 0) {
            iterator = futures.iterator();
            while (iterator.hasNext()) {
                future = iterator.next();
                if (future.isDone()) {
                    tempPrefixes = future.get();
                    if (tempPrefixes != null) listForNextIteratively(tempPrefixes);
                    iterator.remove();
                }
            }
        }
    }

    /**
     * 根据当前参数值创建多线程执行数据源导出工作
     */
    @Override
    public void export() throws Exception {
        String info = processor == null ? String.join(" ", "list objects from upyun bucket:", bucket) :
                String.join(" ", "list objects from upyun bucket:", bucket, "and", processor.getProcessName());
        rootLogger.info("{} running...", info);
        rootLogger.info("order\tprefix\tquantity");
        showdownHook();
        if (prefixes == null || prefixes.size() == 0) {
            IStorageLister<FileItem> startLister = generateLister("");
            listing(startLister);
            if (startLister.getDirectories() == null || startLister.getDirectories().size() <= 0) {
                prefixes = null;
            } else if (hasAntiPrefixes) {
                prefixes = startLister.getDirectories().parallelStream()
                        .filter(this::checkPrefix).peek(this::recordListerByPrefix).collect(Collectors.toList());
            } else {
                startLister.getDirectories().parallelStream().forEach(this::recordListerByPrefix);
                prefixes = startLister.getDirectories();
            }
        } else {
            if (hasAntiPrefixes) {
                prefixes = prefixes.stream().filter(this::checkPrefix).map(prefix -> {
                    if (prefix.endsWith("/")) return prefix.substring(0, prefix.length() - 1);
                    return prefix;
                }).collect(Collectors.toList());
            } else {
                prefixes = prefixes.stream().map(prefix -> {
                    if (prefix.endsWith("/")) return prefix.substring(0, prefix.length() - 1);
                    return prefix;
                }).collect(Collectors.toList());
            }
        }
        try {
            if (prefixes != null && prefixes.size() > 0) {
                executorPool = Executors.newFixedThreadPool(threads);
                listForNextIteratively(prefixes);
                executorPool.shutdown();
                while (!executorPool.isTerminated()) sleep(1000);
            }
            rootLogger.info("{} finished, results in {}.", info, savePath);
            endAction();
        } catch (Throwable e) {
            executorPool.shutdownNow();
            rootLogger.error(e.toString(), e);
            endAction();
            System.exit(-1);
        }
    }
}
