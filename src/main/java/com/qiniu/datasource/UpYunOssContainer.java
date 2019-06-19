package com.qiniu.datasource;

import com.qiniu.common.QiniuException;
import com.qiniu.common.SuitsException;
import com.qiniu.convert.UOSObjToMap;
import com.qiniu.convert.UOSObjToString;
import com.qiniu.entry.CommonParams;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.persistence.IResultOutput;
import com.qiniu.sdk.FileItem;
import com.qiniu.sdk.UpYunClient;
import com.qiniu.sdk.UpYunConfig;
import com.qiniu.util.HttpRespUtils;
import com.qiniu.util.LineUtils;
import com.qiniu.util.SystemUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class UpYunOssContainer implements IDataSource<ILister<FileItem>, IResultOutput<BufferedWriter>, Map<String, String>> {

    private String username;
    private String password;
    private UpYunConfig configuration;
    protected String bucket;
    private List<String> antiPrefixes;
    private Map<String, String[]> prefixesMap;
    private List<String> prefixes;
//    private boolean prefixLeft;
//    private boolean prefixRight;
    protected Map<String, String> indexMap;
    protected int unitLen;
    private int threads;
    protected int retryTimes = 5;
    protected String savePath;
    protected boolean saveTotal;
    protected String saveFormat;
    protected String saveSeparator;
    protected Set<String> rmFields;
    private ExecutorService executorPool; // 线程池
    private AtomicBoolean exitBool; // 多线程的原子操作 bool 值
    private ILineProcess<Map<String, String>> processor; // 定义的资源处理器

    public UpYunOssContainer(String username, String password, UpYunConfig configuration, String bucket,
                             List<String> antiPrefixes, Map<String, String[]> prefixesMap,
//                             boolean prefixLeft, boolean prefixRight,
                             Map<String, String> indexMap, int unitLen, int threads) {
        this.username = username;
        this.password = password;
        this.configuration = configuration;
        this.bucket = bucket;
        // 先设置 antiPrefixes 后再设置 prefixes，因为可能需要从 prefixes 中去除 antiPrefixes 含有的元素
        this.antiPrefixes = antiPrefixes;
        setPrefixesAndMap(prefixesMap);
//        this.prefixLeft = prefixLeft;
//        this.prefixRight = prefixRight;
        setIndexMapWithDefault(indexMap);
        this.unitLen = unitLen;
        this.threads = threads;
        this.saveTotal = true; // 默认全记录保存
    }

    // 不调用则各参数使用默认值
    public void setSaveOptions(String savePath, boolean saveTotal, String format, String separator, Set<String> rmFields) {
        this.savePath = savePath;
        this.saveTotal = saveTotal;
        this.saveFormat = format;
        this.saveSeparator = separator;
        this.rmFields = rmFields;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    private void setIndexMapWithDefault(Map<String, String> indexMap) {
        if (indexMap == null || indexMap.size() == 0) {
            if (this.indexMap == null) this.indexMap = new HashMap<>();
            for (String fileInfoField : LineUtils.fileInfoFields) {
                this.indexMap.put(fileInfoField, fileInfoField);
            }
        } else {
            this.indexMap = indexMap;
        }
    }

    // 通过 commonParams 来更新基本参数
    public void updateSettings(CommonParams commonParams) {
        bucket = commonParams.getBucket();
        antiPrefixes = commonParams.getAntiPrefixes();
        setPrefixesAndMap(commonParams.getPrefixesMap());
//        prefixLeft = commonParams.getPrefixLeft();
//        prefixRight = commonParams.getPrefixRight();
        setIndexMapWithDefault(commonParams.getIndexMap());
        unitLen = commonParams.getUnitLen();
        retryTimes = commonParams.getRetryTimes();
        threads = commonParams.getThreads();
        savePath = commonParams.getSavePath();
        saveTotal = commonParams.getSaveTotal();
        saveFormat = commonParams.getSaveFormat();
        saveSeparator = commonParams.getSaveSeparator();
        rmFields = commonParams.getRmFields();
    }

    public void setProcessor(ILineProcess<Map<String, String>> processor) {
        this.processor = processor;
    }

    private void setPrefixesAndMap(Map<String, String[]> prefixesMap) {
        if (prefixesMap == null) {
            this.prefixesMap = new HashMap<>();
        } else {
            this.prefixesMap = prefixesMap;
            prefixes = prefixesMap.keySet().parallelStream().filter(this::checkPrefix)
                    .sorted().collect(Collectors.toList());
            int size = prefixes.size();
            if (size == 0) return;
            Iterator<String> iterator = prefixes.iterator();
            String temp = iterator.next();
            while (iterator.hasNext() && size > 0) {
                size--;
                String prefix = iterator.next();
                if (prefix.startsWith(temp)) {
                    iterator.remove();
                    this.prefixesMap.remove(prefix);
                } else {
                    temp = prefix;
                }
            }
        }
    }

    @Override
    public String getSourceName() {
        return "upyun";
    }

    /**
     * 检验 prefix 是否有效，在 antiPrefixes 前缀列表中或者为空均无效
     * @param prefix 待检验的 prefix
     * @return 检验结果，true 表示 prefix 有效不需要剔除
     */
    private boolean checkPrefix(String prefix) {
        if (prefix == null || "".equals(prefix)) return false;
        if (antiPrefixes == null) antiPrefixes = new ArrayList<>();
        for (String antiPrefix : antiPrefixes) {
            if (prefix.startsWith(antiPrefix)) return false;
        }
        return true;
    }

    protected ITypeConvert<FileItem, Map<String, String>> getNewConverter() {
        return new UOSObjToMap(indexMap);
    }

    protected ITypeConvert<FileItem, String> getNewStringConverter() throws IOException {
        return new UOSObjToString(saveFormat, saveSeparator, rmFields);
    }

    protected IResultOutput<BufferedWriter> getNewResultSaver(String order) throws IOException {
        return order != null ? new FileSaveMapper(savePath, getSourceName(), order) : new FileSaveMapper(savePath);
    }

    /**
     * 执行列举操作，直到当前的 lister 列举结束，并使用 processor 对象执行处理过程
     * @param lister 已经初始化的 lister 对象
     * @param saver 用于列举结果持久化的文件对象
     * @param processor 用于资源处理的处理器对象
     * @throws IOException 列举出现错误或者持久化错误抛出的异常
     */
    public void export(ILister<FileItem> lister, IResultOutput<BufferedWriter> saver, ILineProcess<Map<String, String>> processor) throws IOException {
        ITypeConvert<FileItem, Map<String, String>> converter = getNewConverter();
        ITypeConvert<FileItem, String> stringConverter = getNewStringConverter();
        List<Map<String, String>> convertedList;
        List<String> writeList;
        List<FileItem> objects = lister.currents();
        int retry;
        boolean goon = objects.size() > 0 || lister.hasNext();
        // 初始化的 lister 包含首次列举的结果列表，需要先取出，后续向前列举时会更新其结果列表
        while (goon) {
            if (saveTotal) {
                writeList = stringConverter.convertToVList(objects);
                if (writeList.size() > 0) saver.writeSuccess(String.join("\n", writeList), false);
                if (stringConverter.errorSize() > 0) saver.writeKeyFile("string-error", stringConverter.errorLines(), false);
            }
            // 如果抛出异常需要检测下异常是否是可继续的异常，如果是程序可继续的异常，忽略当前异常保持数据源读取过程继续进行
            try {
                if (processor != null) {
                    convertedList = converter.convertToVList(objects);
                    if (converter.errorSize() > 0) saver.writeError(converter.errorLines(), false);
                    processor.processLine(convertedList);
                }
            } catch (QiniuException e) {
                if (HttpRespUtils.checkException(e, 2) < -1) throw e;
            }
            retry = retryTimes;
            goon = lister.hasNext();
            while (goon) {
                try {
                    lister.listForward();
                    objects = lister.currents();
                    break;
                } catch (SuitsException e) {
                    System.out.println("list objects by prefix:" + lister.getPrefix() + " retrying...\n" + e.getMessage());
                    if (e.getStatusCode() == 401 && e.getMessage().contains("date offset error")) retry--;
                    else if (e.getStatusCode() == 429) {
                        try { Thread.sleep(3000); } catch (InterruptedException ignored) { }
                    } else if (HttpRespUtils.checkStatusCode(e.getStatusCode()) < 0) throw e;
                    else if (retry <= 0 && e.getStatusCode() >= 500) throw e;
                    else retry--;
                }
            }
        }
    }

    /**
     * 在 prefixes map 的参数配置中取出 marker 和 end 参数
     * @param prefix 配置的前缀参数
     * @return 返回针对该前缀配置的 marker 和 end
     */
    private String[] getMarkerAndEnd(String prefix) {
        if (prefixesMap.containsKey(prefix)) {
            String[] mapValue = prefixesMap.get(prefix);
            if (mapValue != null && mapValue.length > 1) {
                return mapValue;
            } else if (mapValue == null || mapValue.length == 0){
                return new String[]{"", ""};
            } else {
                return new String[]{mapValue[0], ""};
            }
        } else {
            return new String[]{"", ""};
        }
    }

    private UpLister generateLister(String prefix) throws SuitsException {
        int retry = retryTimes;
        String[] markerAndEnd = getMarkerAndEnd(prefix);
        while (true) {
            try {
                return new UpLister(new UpYunClient(configuration, username, password), bucket, prefix, markerAndEnd[0],
                        markerAndEnd[1], unitLen);
            } catch (SuitsException e) {
                System.out.println("generate lister by prefix:" + prefix + " retrying...\n" + e.getMessage());
                if (e.getStatusCode() == 401 && e.getMessage().contains("date offset error")) retry--;
                else if (e.getStatusCode() == 429) {
                    try { Thread.sleep(3000); } catch (InterruptedException ignored) { }
                }
                else if (HttpRespUtils.checkStatusCode(e.getStatusCode()) < 0) throw e;
                else if (retry <= 0 && e.getStatusCode() >= 500) throw e;
                else retry--;
            }
        }
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

    private List<String> listing(UpLister lister, int order) throws Exception {
        ILineProcess<Map<String, String>> lineProcessor = processor == null ? null : processor.clone();
        // 持久化结果标识信息
        String newOrder = String.valueOf(order);
        IResultOutput<BufferedWriter> saver = getNewResultSaver(newOrder);
        try {
            String record = "order " + newOrder + ": " + lister.getPrefix();
            export(lister, saver, processor);
            record += "\tsuccessfully done";
            System.out.println(record);
        } catch (Exception e) {
            System.out.println("order " + newOrder + ": " + lister.getPrefix() + "\tmarker: " +
                    lister.getMarker() + "\tend:" + lister.getEndPrefix());
            e.printStackTrace();
        } finally {
            lister.close();
            saver.closeWriters();
            if (lineProcessor != null) lineProcessor.closeResource();
        }
        return lister.getDirectories();
    }

    /**
     * 根据当前参数值创建多线程执行数据源导出工作
     */
    public void export() {
        String info = "list objects from bucket: " + bucket + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " running...");
        AtomicInteger order = new AtomicInteger(0);
        executorPool = Executors.newFixedThreadPool(threads);
        exitBool = new AtomicBoolean(false);
        try {
            if (prefixes == null || prefixes.size() == 0) {
                UpLister startLister = generateLister("");
                prefixes = listing(startLister, order.addAndGet(1));
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
                                UpLister upLister = generateLister(prefix);
                                if (upLister.hasNext() || upLister.getDirectories() != null) {
                                    return listing(upLister, order.addAndGet(1));
                                } else {
                                    executorPool.execute(() -> {
                                        try {
                                            listing(upLister, order.addAndGet(1));
                                        } catch (Exception e) {
                                            SystemUtils.exit(exitBool, e);
                                        }
                                    });
                                    return null;
                                }
//                                if (!upLister.hasNext() && upLister.getDirectories() == null) {
//                                    executorPool.execute(() -> {
//                                        try {
//                                            listingResult(upLister, map.get(preOne));
////                                            listingResult(upLister, order);
//                                        } catch (Exception e) {
//                                            SystemUtils.exit(exitBool, e);
//                                        }
//                                    });
//                                    return null;
//                                } else {
//                                    return listingResult(upLister, map.get(preOne));
////                                    return listingResult(upLister, order);
//                                }
                            } catch (Exception e) {
                                SystemUtils.exit(exitBool, e);
                                return null;
                            }
                        }).filter(Objects::nonNull)
                        .reduce((list1, list2) -> { list1.addAll(list2); return list1; }).orElse(null);
                loopMore.set(false);
            }

//            prefixes.parallelStream().filter(this::checkPrefix)
//                    .forEach(prefix -> {
//                        executorPool.execute(() -> {
//                            try {
//                                UpLister upLister = generateLister(prefix);
//                                listingResult(upLister, order);
//                            } catch (Exception e) {
//                                SystemUtils.exit(exitBool, e);
//                            }
//                        });
//                    });
            executorPool.shutdown();
            while (!executorPool.isTerminated()) Thread.sleep(1000);
            System.out.println(info + " finished");
        } catch (Throwable throwable) {
            SystemUtils.exit(exitBool, throwable);
        }
    }
}
