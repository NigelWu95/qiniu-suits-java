package com.qiniu.datasource;

import com.google.gson.JsonObject;
import com.qiniu.common.SuitsException;
import com.qiniu.convert.YOSObjToMap;
import com.qiniu.convert.YOSObjToString;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.persistence.IResultOutput;
import com.qiniu.sdk.FileItem;
import com.qiniu.sdk.UpYunClient;
import com.qiniu.sdk.UpYunConfig;
import com.qiniu.util.JsonUtils;
import com.qiniu.util.ListingUtils;
import com.qiniu.util.UniOrderUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UpYosContainer extends CloudStorageContainer<FileItem, BufferedWriter, Map<String, String>> {

    private String username;
    private String password;
    private UpYunConfig configuration;

    public UpYosContainer(String username, String password, UpYunConfig configuration, String bucket,
                          List<String> antiPrefixes, Map<String, Map<String, String>> prefixesMap,
//                             boolean prefixLeft, boolean prefixRight,
                          Map<String, String> indexMap, int unitLen, int threads) {
        super(bucket, antiPrefixes, prefixesMap, false, false, indexMap, unitLen, threads);
        this.username = username;
        this.password = password;
        this.configuration = configuration;
    }

    @Override
    public String getSourceName() {
        return "upyun";
    }

    @Override
    protected ITypeConvert<FileItem, Map<String, String>> getNewConverter() {
        return new YOSObjToMap(indexMap);
    }

    @Override
    protected ITypeConvert<FileItem, String> getNewStringConverter() throws IOException {
        return new YOSObjToString(saveFormat, saveSeparator, rmFields);
    }

    @Override
    protected IResultOutput<BufferedWriter> getNewResultSaver(String order) throws IOException {
        return order != null ? new FileSaveMapper(savePath, getSourceName(), order) : new FileSaveMapper(savePath);
    }

    @Override
    protected ILister<FileItem> getLister(String prefix, String marker, String start, String end) throws SuitsException {
        if (marker == null || "".equals(marker)) marker = ListingUtils.getUpYunMarker(username, password, bucket, start);
        return new UpLister(new UpYunClient(configuration, username, password), bucket, prefix, marker, end, unitLen);
    }

    private List<ILister<FileItem>> getListerListByPrefixes(List<String> prefixes) {
        for (String prefix : prefixes) {
            JsonObject json = prefixesMap.get(prefix) == null ? null :
                    JsonUtils.toJsonObject(JsonUtils.toJsonWithoutUrlEscape(prefixesMap.get(prefix)));
            ListingUtils.recordPrefixConfig(prefix, json);
        }
        return prefixes.parallelStream().filter(this::checkPrefix)
                .map(prefix -> {
                    try {
                        UpLister upLister = (UpLister) generateLister(prefix);
                        int order = UniOrderUtils.getOrder();
                        if (upLister.hasNext() || upLister.getDirectories() != null) {
                            listing(upLister, order);
                            return upLister;
                        } else {
                            executorPool.execute(() -> listing(upLister, order));
                            return null;
                        }
                    } catch (SuitsException e) {
                        JsonObject json = prefixesMap.get(prefix) == null ? null :
                                JsonUtils.toJsonObject(JsonUtils.toJsonWithoutUrlEscape(prefixesMap.get(prefix)));
                        ListingUtils.recordPrefixConfig(prefix, json);
                        System.out.println("generate lister failed by " + prefix + "\t" + json);
                        e.printStackTrace(); return null;
                    }
                }).filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * 根据当前参数值创建多线程执行数据源导出工作
     */
    @Override
    public void export() throws Exception {
        String info = "list objects from bucket: " + bucket + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " running...");
        executorPool = Executors.newFixedThreadPool(threads);
        if (prefixes == null || prefixes.size() == 0) {
            UpLister startLister = (UpLister) generateLister("");
            int order = UniOrderUtils.getOrder();
            listing(startLister, order);
            prefixes = startLister.getDirectories();
        }
        List<ILister<FileItem>> listerList = null;
        try {
            listerList = getListerListByPrefixes(prefixes);
            while (listerList != null && listerList.size() > 0) {
                prefixes = listerList.parallelStream().map(lister -> ((UpLister) lister).getDirectories())
                        .filter(Objects::nonNull)
                        .reduce((list1, list2) -> { list1.addAll(list2); return list1; }).orElse(null);
                if (prefixes == null || prefixes.size() == 0) {
                    listerList = null;
                } else {
                    listerList = getListerListByPrefixes(prefixes);
                }
            }
//            while (prefixes != null && prefixes.size() > 0) {
//                prefixes = prefixes.parallelStream().filter(this::checkPrefix).map(prefix -> {
//                    try {
//                        UpLister upLister = (UpLister) generateLister(prefix);
//                        int order = UniOrderUtils.getOrder();
//                        if (upLister.hasNext() || upLister.getDirectories() != null) {
//                            listing(upLister, order);
//                            return upLister.getDirectories();
//                        } else {
//                            executorPool.execute(() -> listing(upLister, order));
//                            return null;
//                        }
//                    } catch (SuitsException e) {
//                        JsonObject json = prefixesMap.get(prefix) == null ? null :
//                                JsonUtils.toJsonObject(JsonUtils.toJsonWithoutUrlEscape(prefixesMap.get(prefix)));
//                        ListingUtils.recordPrefixConfig(prefix, json);
//                        System.out.println("generate lister failed by " + prefix + "\t" + json);
//                        e.printStackTrace(); return null;
//                    }
//                }).filter(Objects::nonNull).reduce((list1, list2) -> { list1.addAll(list2); return list1; }).orElse(null);
//            }
            executorPool.shutdown();
            while (!executorPool.isTerminated())
                try { Thread.sleep(1000); } catch (InterruptedException ignored) { Thread.sleep(1000); }
            System.out.println(info + " finished");
        } catch (Throwable e) {
            executorPool.shutdownNow();
            e.printStackTrace();
            if (listerList != null) {
                for (ILister<FileItem> lister : listerList) {
                    if (lister.currents() != null) {
                        JsonObject json = ListingUtils.continuePrefixConf(lister);
                        if (json != null) ListingUtils.recordPrefixConfig(lister.getPrefix(), json);
                    }
                }
            }
        } finally {
            ListingUtils.writeContinuedPrefixConfig(savePath);
        }
    }
}
