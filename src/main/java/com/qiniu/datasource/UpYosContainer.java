package com.qiniu.datasource;

import com.qiniu.common.SuitsException;
import com.qiniu.convert.Converter;
import com.qiniu.convert.JsonObjectPair;
import com.qiniu.convert.StringMapPair;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.interfaces.IStringFormat;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.persistence.IResultOutput;
import com.qiniu.sdk.FileItem;
import com.qiniu.sdk.UpYunClient;
import com.qiniu.sdk.UpYunConfig;
import com.qiniu.util.CloudAPIUtils;
import com.qiniu.util.LineUtils;
import com.qiniu.util.UniOrderUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class UpYosContainer extends CloudStorageContainer<FileItem, BufferedWriter, Map<String, String>> {

    private String username;
    private String password;
    private UpYunConfig configuration;
    private Map<String, String> indexPair;
    private List<String> fields;

    public UpYosContainer(String username, String password, UpYunConfig configuration, String bucket,
                          List<String> antiPrefixes, Map<String, Map<String, String>> prefixesMap,
//                             boolean prefixLeft, boolean prefixRight,
                          Map<String, String> indexMap, int unitLen, int threads) throws SuitsException {
        super(bucket, antiPrefixes, prefixesMap, false, false, indexMap, unitLen, threads);
        this.username = username;
        this.password = password;
        this.configuration = configuration;
        UpLister upLister = new UpLister(new UpYunClient(configuration, username, password), bucket, null,
                null, null, 1);
        upLister.close();
        upLister = null;
        indexPair = LineUtils.getReversedIndexMap(indexMap, rmFields);
        for (String etagField : LineUtils.etagFields) indexPair.remove(etagField);
        for (String typeField : LineUtils.typeFields) indexPair.remove(typeField);
        for (String statusField : LineUtils.statusFields) indexPair.remove(statusField);
        for (String md5Field : LineUtils.md5Fields) indexPair.remove(md5Field);
        for (String ownerField : LineUtils.ownerFields) indexPair.remove(ownerField);
        fields = new ArrayList<>();
        for (String defaultFileField : LineUtils.defaultFileFields) {
            if (indexPair.containsKey(defaultFileField)) fields.add(defaultFileField);
        }
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
                return LineUtils.toPair(line, indexPair, new StringMapPair());
            }
        };
    }

    @Override
    protected ITypeConvert<FileItem, String> getNewStringConverter() throws IOException {
        IStringFormat<FileItem> stringFormatter;
        if ("json".equals(saveFormat)) {
            stringFormatter = line -> LineUtils.toPair(line, indexPair, new JsonObjectPair()).toString();
        } else if ("csv".equals(saveFormat)) {
            stringFormatter = line -> LineUtils.toFormatString(line, ",", fields);
        } else if ("tab".equals(saveFormat)) {
            stringFormatter = line -> LineUtils.toFormatString(line, saveSeparator, fields);
        } else {
            throw new IOException("please check your format for map to string.");
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
    protected ILister<FileItem> getLister(String prefix, String marker, String start, String end) throws SuitsException {
        if (marker == null || "".equals(marker)) marker = CloudAPIUtils.getUpYunMarker(username, password, bucket, start);
        return new UpLister(new UpYunClient(configuration, username, password), bucket, prefix, marker, end, unitLen);
    }

    private List<String> listAndGetNextPrefixes(List<String> prefixes) {
        return prefixes.parallelStream().map(prefix -> {
                UpLister upLister;
                try {
                    upLister = (UpLister) generateLister(prefix);
                } catch (SuitsException e) {
                    System.out.println("generate lister failed by " + prefix + "\t" + prefixesMap.get(prefix));
                    e.printStackTrace(); return null;
                }
                if (upLister.hasNext() || upLister.getDirectories() != null) {
                    listing(upLister, UniOrderUtils.getOrder());
                    if (upLister.getDirectories() == null || upLister.getDirectories().size() <= 0) return null;
                    else if (hasAntiPrefixes) return upLister.getDirectories().stream()
                            .filter(this::checkPrefix).peek(this::recordListerByPrefix).collect(Collectors.toList());
                    else {
                        for (String dir : upLister.getDirectories()) recordListerByPrefix(dir);
                        return upLister.getDirectories();
                    }
                } else {
                    executorPool.execute(() -> listing(upLister, UniOrderUtils.getOrder()));
                    return null;
                }
            }).filter(Objects::nonNull)
            .reduce((list1, list2) -> { list1.addAll(list2); return list1; }).orElse(null);
    }

    /**
     * 根据当前参数值创建多线程执行数据源导出工作
     */
    @Override
    public void export() throws Exception {
        String info = "list objects from bucket: " + bucket + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " running...");
        if (prefixes == null || prefixes.size() == 0) {
            UpLister startLister = (UpLister) generateLister("");
            int order = UniOrderUtils.getOrder();
            listing(startLister, order);
            if (startLister.getDirectories() == null || startLister.getDirectories().size() <= 0) return;
            else if (hasAntiPrefixes) prefixes = startLister.getDirectories().parallelStream()
                    .filter(this::checkPrefix).peek(this::recordListerByPrefix).collect(Collectors.toList());
            else {
                for (String dir : startLister.getDirectories()) recordListerByPrefix(dir);
                prefixes = startLister.getDirectories();
            }
        }
        executorPool = Executors.newFixedThreadPool(threads);
        try {
            prefixes = listAndGetNextPrefixes(prefixes);
            while (prefixes != null && prefixes.size() > 0) {
                prefixes = listAndGetNextPrefixes(prefixes);
            }
            executorPool.shutdown();
            while (!executorPool.isTerminated()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                    int i = 0;
                    while (i < 1000) i++;
                }
            }
            System.out.println(info + " finished.");
        } catch (Throwable e) {
            executorPool.shutdownNow();
            e.printStackTrace();
            ILineProcess<Map<String, String>> processor;
            for (Map.Entry<String, IResultOutput<BufferedWriter>> saverEntry : saverMap.entrySet()) {
                saverEntry.getValue().closeWriters();
                processor = processorMap.get(saverEntry.getKey());
                if (processor != null) processor.closeResource();
            }
        } finally {
            writeContinuedPrefixConfig(savePath, "prefixes");
        }
    }
}
