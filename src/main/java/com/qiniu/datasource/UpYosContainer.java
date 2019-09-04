package com.qiniu.datasource;

import com.qiniu.common.SuitsException;
import com.qiniu.convert.Converter;
import com.qiniu.convert.JsonObjectPair;
import com.qiniu.convert.StringBuilderPair;
import com.qiniu.convert.StringMapPair;
import com.qiniu.interfaces.ILister;
import com.qiniu.interfaces.IStringFormat;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.interfaces.IResultOutput;
import com.qiniu.sdk.FileItem;
import com.qiniu.sdk.UpYunClient;
import com.qiniu.sdk.UpYunConfig;
import com.qiniu.util.CloudApiUtils;
import com.qiniu.util.ConvertingUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
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
    protected ILister<FileItem> getLister(String prefix, String marker, String start, String end) throws SuitsException {
        if (marker == null || "".equals(marker)) marker = CloudApiUtils.getUpYunMarker(username, password, bucket, start);
        return new UpLister(new UpYunClient(configuration, username, password), bucket, prefix, marker, end, unitLen);
    }

    private List<String> listAndGetNextPrefixes(List<String> prefixes) {
        return prefixes.parallelStream().map(prefix -> {
                UpLister upLister;
                try {
                    upLister = (UpLister) generateLister(prefix);
                } catch (SuitsException e) {
                    logger.error("generate lister failed by {}\t{}", prefix, prefixesMap.get(prefix), e);
                    return null;
                }
                if (upLister.hasNext() || upLister.getDirectories() != null) {
                    listing(upLister);
                    if (upLister.getDirectories() == null || upLister.getDirectories().size() <= 0) {
                        return null;
                    } else if (hasAntiPrefixes) {
                        return upLister.getDirectories().stream().filter(this::checkPrefix)
                                .peek(this::recordListerByPrefix).collect(Collectors.toList());
                    } else {
                        for (String dir : upLister.getDirectories()) recordListerByPrefix(dir);
                        return upLister.getDirectories();
                    }
                } else {
                    executorPool.execute(() -> listing(upLister));
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
        logger.info("{} running...", info);
        if (prefixes == null || prefixes.size() == 0) {
            UpLister startLister = (UpLister) generateLister("");
            listing(startLister);
            if (startLister.getDirectories() == null || startLister.getDirectories().size() <= 0) {
                logger.info("{} finished.", info);
                return;
            } else if (hasAntiPrefixes) {
                prefixes = startLister.getDirectories().parallelStream()
                        .filter(this::checkPrefix).peek(this::recordListerByPrefix).collect(Collectors.toList());
            } else {
                for (String dir : startLister.getDirectories()) recordListerByPrefix(dir);
                prefixes = startLister.getDirectories();
            }
        } else {
            prefixes = prefixes.stream().map(prefix -> {
                if (prefix.endsWith("/")) return prefix.substring(0, prefix.length() - 1);
                return prefix;
            }).collect(Collectors.toList());
        }
        executorPool = Executors.newFixedThreadPool(threads);
        showdownHook();
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
            logger.info("{} finished.", info);
            endAction();
        } catch (Throwable e) {
            executorPool.shutdownNow();
            logger.error(e.toString(), e);
            endAction();
            System.exit(-1);
        }
    }
}
