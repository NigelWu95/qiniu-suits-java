package com.qiniu.datasource;

import com.qiniu.common.SuitsException;
import com.qiniu.convert.YOSObjToMap;
import com.qiniu.convert.YOSObjToString;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.persistence.IResultOutput;
import com.qiniu.sdk.FileItem;
import com.qiniu.sdk.UpYunClient;
import com.qiniu.sdk.UpYunConfig;
import com.qiniu.util.ListingUtils;
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

    private List<UpLister> getListerListByPrefixes(List<String> prefixes) {
        for (String prefix : prefixes) recordListerByPrefix(prefix);
        return prefixes.parallelStream().filter(this::checkPrefix)
                .map(prefix -> {
                    UpLister upLister;
                    try {
                        upLister = (UpLister) generateLister(prefix);
                    } catch (SuitsException e) {
                        System.out.println("generate lister failed by " + prefix + "\t" + prefixesMap.get(prefix));
                        e.printStackTrace(); return null;
                    }
                    int order = UniOrderUtils.getOrder();
                    if (upLister.hasNext() || upLister.getDirectories() != null) {
                        listing(upLister, order);
                        return upLister;
                    } else {
                        executorPool.execute(() -> listing(upLister, order));
                        return null;
                    }
                }).filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private void concurrentListing() throws IOException {
        executorPool = Executors.newFixedThreadPool(threads);
        List<UpLister> listerList = null;
        try {
            listerList = getListerListByPrefixes(prefixes);
            while (listerList != null && listerList.size() > 0) {
                prefixes = listerList.parallelStream().map(UpLister::getDirectories)
                        .filter(Objects::nonNull)
                        .reduce((list1, list2) -> { list1.addAll(list2); return list1; }).orElse(null);
                if (prefixes == null || prefixes.size() == 0) {
                    listerList = null;
                } else {
                    listerList = getListerListByPrefixes(prefixes);
                }
            }
            executorPool.shutdown();
            while (!executorPool.isTerminated())
                try { Thread.sleep(1000); } catch (InterruptedException ignored) { Thread.sleep(1000); }
            System.out.println("finished.");
        } catch (Throwable e) {
            executorPool.shutdownNow();
            e.printStackTrace();
            List<String> directories;
            if (listerList != null) {
                for (UpLister lister : listerList) {
                    directories = lister.getDirectories();
                    if (directories != null) {
                        for (String directory : directories) recordListerByPrefix(directory);
                    }
                }
            }
        } finally {
            writeContinuedPrefixConfig(savePath, "prefixes");
        }
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
            prefixes = startLister.getDirectories();
        }
        concurrentListing();
    }
}
