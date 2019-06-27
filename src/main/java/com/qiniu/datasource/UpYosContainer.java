package com.qiniu.datasource;

import com.qiniu.common.SuitsException;
import com.qiniu.convert.YOSObjToMap;
import com.qiniu.convert.YOSObjToString;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.persistence.IResultOutput;
import com.qiniu.sdk.FileItem;
import com.qiniu.sdk.UpYunClient;
import com.qiniu.sdk.UpYunConfig;
import com.qiniu.util.OssUtils;
import com.qiniu.util.ThrowUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

//public class UpYosContainer implements IDataSource<ILister<FileItem>, IResultOutput<BufferedWriter>, Map<String, String>> {
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
        if (marker == null || "".equals(marker)) marker = OssUtils.getUpYunMarker(username, password, bucket, start);
        return new UpLister(new UpYunClient(configuration, username, password), bucket, prefix, marker, end, unitLen);
    }

//    private void recursionListing(UpLister lister, IResultOutput<BufferedWriter> saver,
//                                  ILineProcess<Map<String, String>> processor) throws IOException {
//        export(lister, saver, processor);
//        lister.close();
//        List<String> directories = lister.getDirectories();
//        if (directories != null) {
//            for (String prefix : directories) {
//                if (checkPrefix(prefix)) {
//                    UpLister upLister = generateLister(lister.getPrefix() + "/" + prefix);
//                    recursionListing(upLister, saver, processor);
//                }
//            }
//        }
//    }

    private List<String> listing(UpLister lister, AtomicInteger order) throws Exception {
        ILineProcess<Map<String, String>> lineProcessor = processor == null ? null : processor.clone();
        Integer ord = order.addAndGet(1);
        Integer rem = ord % 5000;
        while (ord > 5000) {
            if (orderMap.remove(rem) != null) ord = rem;
        }
        // 持久化结果标识信息
        String orderStr = String.valueOf(ord);
        IResultOutput<BufferedWriter> saver = getNewResultSaver(orderStr);
        try {
            String record = "order " + orderStr + ": " + lister.getPrefix();
            export(lister, saver, lineProcessor);
            record += "\tsuccessfully done";
            System.out.println(record);
        } catch (Exception e) {
            System.out.println("order " + orderStr + ": " + lister.getPrefix() + "\tmarker: " +
                    lister.getMarker() + "\tend:" + lister.getEndPrefix());
            throw e;
        } finally {
            lister.close();
            saver.closeWriters();
            if (lineProcessor != null) lineProcessor.closeResource();
        }
        orderMap.put(ord, ord);
        return lister.getDirectories();
    }

    /**
     * 根据当前参数值创建多线程执行数据源导出工作
     */
    @Override
    public void export() {
        String info = "list objects from bucket: " + bucket + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " running...");
        AtomicInteger order = new AtomicInteger(0);
        executorPool = Executors.newFixedThreadPool(threads);
        exitBool = new AtomicBoolean(false);
        orderMap = new ConcurrentHashMap<>();
        try {
            if (prefixes == null || prefixes.size() == 0) {
                UpLister startLister = (UpLister) generateLister("");
                prefixes = listing(startLister, order);
            }
//            else {
//                if (prefixLeft) {
//                    String minPrefix = prefixes.get(0);
//                    UpLister firstLister = generateLister("");
//                    firstLister.setEndPrefix(minPrefix);
//                    listing(firstLister, order);
//                    firstLister.getDirectories().parallelStream().filter(prefix -> prefix.compareTo(minPrefix) < 0)
//                            .forEach(prefixes::add);
//                }
//                if (prefixRight) {
//
//                }
//            }

            AtomicBoolean loopMore = new AtomicBoolean(true);
            while (prefixes != null && prefixes.size() > 0) {
                prefixes = prefixes.parallelStream().filter(this::checkPrefix)
                        .map(prefix -> {
                            try {
                                UpLister upLister = (UpLister) generateLister(prefix);
                                if (upLister.hasNext() || upLister.getDirectories() != null) {
                                    return listing(upLister, order);
                                } else {
                                    executorPool.execute(() -> {
                                        try {
                                            listing(upLister, order);
                                        } catch (Exception e) {
                                            ThrowUtils.exit(exitBool, e);
                                        }
                                    });
                                    return null;
                                }
                            } catch (Exception e) {
                                ThrowUtils.exit(exitBool, e);
                                return null;
                            }
                        }).filter(Objects::nonNull)
                        .reduce((list1, list2) -> { list1.addAll(list2); return list1; }).orElse(null);
                loopMore.set(false);
            }
            executorPool.shutdown();
            while (!executorPool.isTerminated()) Thread.sleep(1000);
            System.out.println(info + " finished");
        } catch (Throwable throwable) {
            ThrowUtils.exit(exitBool, throwable);
        }
    }
}
