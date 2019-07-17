package com.qiniu.datasource;

import com.google.gson.JsonObject;
import com.qiniu.common.QiniuException;
import com.qiniu.common.SuitsException;
import com.qiniu.entry.CommonParams;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.persistence.IResultOutput;
import com.qiniu.util.*;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class CloudStorageContainer<E, W, T> implements IDataSource<ILister<E>, IResultOutput<W>, T> {

    protected String bucket;
    protected List<String> antiPrefixes;
    protected Map<String, Map<String, String>> prefixesMap;
    protected List<String> prefixes;
    protected boolean prefixLeft;
    protected boolean prefixRight;
    protected Map<String, String> indexMap;
    protected int unitLen;
    protected int threads;
    protected int retryTimes = 5;
    protected String savePath;
    protected boolean saveTotal;
    protected String saveFormat;
    protected String saveSeparator;
    protected Set<String> rmFields;
    protected ExecutorService executorPool; // 线程池
    protected ILineProcess<T> processor; // 定义的资源处理器
    protected List<String> originPrefixList = new ArrayList<>();
    public static String startPoint;
    private ConcurrentMap<String, Map<String, String>> prefixAndEndedMap = new ConcurrentHashMap<>();

    public CloudStorageContainer(String bucket, List<String> antiPrefixes, Map<String, Map<String, String>> prefixesMap,
                                 boolean prefixLeft, boolean prefixRight, Map<String, String> indexMap, int unitLen, int threads) {
        this.bucket = bucket;
        // 先设置 antiPrefixes 后再设置 prefixes，因为可能需要从 prefixes 中去除 antiPrefixes 含有的元素
        this.antiPrefixes = antiPrefixes;
        setPrefixesAndMap(prefixesMap);
        this.prefixLeft = prefixLeft;
        this.prefixRight = prefixRight;
        setIndexMapWithDefault(indexMap);
        this.unitLen = unitLen;
        this.threads = threads;
        this.saveTotal = true; // 默认全记录保存
        // 由于目前指定包含 "|" 字符的前缀列举会导致超时，因此先将该字符及其 ASCII 顺序之前的 "{" 和之后的（"|}~"）统一去掉，从而优化列举的超
        // 时问题，简化前缀参数的设置，也避免为了兼容该字符去修改代码算法
        originPrefixList.addAll(Arrays.asList((" !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMN").split("")));
        originPrefixList.addAll(Arrays.asList(("OPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz").split("")));
        startPoint = originPrefixList.get(0);
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
        this.retryTimes = retryTimes < 1 ? 5 : retryTimes;
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
        prefixLeft = commonParams.getPrefixLeft();
        prefixRight = commonParams.getPrefixRight();
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

    public void setProcessor(ILineProcess<T> processor) {
        this.processor = processor;
    }

    private void setPrefixesAndMap(Map<String, Map<String, String>> prefixesMap) {
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
            String end;
            while (iterator.hasNext() && size > 0) {
                size--;
                String prefix = iterator.next();
                if (prefix.startsWith(temp)) {
                    end = prefixesMap.get(temp).get("end");
                    if (end == null || "".equals(end)) {
                        iterator.remove();
                        this.prefixesMap.remove(prefix);
                    }
                } else {
                    temp = prefix;
                }
            }
        }
    }

    private synchronized void insertIntoPrefixesMap(String prefix, Map<String, String> markerAndEnd) {
        prefixesMap.put(prefix, markerAndEnd);
    }

    /**
     * 检验 prefix 是否有效，在 antiPrefixes 前缀列表中或者为空均无效
     * @param prefix 待检验的 prefix
     * @return 检验结果，true 表示 prefix 有效不需要剔除
     */
    boolean checkPrefix(String prefix) {
        if (prefix == null || "".equals(prefix)) return false;
        if (antiPrefixes == null) antiPrefixes = new ArrayList<>();
        for (String antiPrefix : antiPrefixes) {
            if (prefix.startsWith(antiPrefix)) return false;
        }
        return true;
    }

    protected abstract ITypeConvert<E, T> getNewConverter();

    protected abstract ITypeConvert<E, String> getNewStringConverter() throws IOException;

    private volatile JsonObject prefixesJson = new JsonObject();

    private synchronized JsonObject continuePrefixConf(ILister lister) {
        JsonObject prefixConf;
        String start = lister.currentStartKey();
        if (start != null) {
            prefixConf = JsonUtils.getOrNew(prefixesJson, lister.getPrefix());
            prefixConf.addProperty("start", start);
        } else if (lister.hasNext()) {
            prefixConf = JsonUtils.getOrNew(prefixesJson, lister.getPrefix());
            prefixConf.addProperty("marker", lister.getMarker());
        } else {
            return null;
        }
        if (lister.getEndPrefix() != null) prefixConf.addProperty("end", lister.getEndPrefix());
        return prefixConf;
    }

    private synchronized void recordPrefixConfig(String prefix, JsonObject continueConf) {
        prefixesJson.add(prefix, continueConf);
    }

    private synchronized void removePrefixConfig(String prefix) {
        prefixesJson.remove(prefix);
    }

    void writeContinuedPrefixConfig(String path, String name) throws IOException {
        if (prefixesJson.size() <= 0) return;
        FileSaveMapper.ext = ".json";
        FileSaveMapper.append = false;
        path = new File(path).getCanonicalPath();
        FileSaveMapper saveMapper = new FileSaveMapper(new File(path).getParent());
//        if (path.endsWith("/")) path = path.substring(0, path.length() - 1);
        String fileName = path.substring(path.lastIndexOf(FileUtils.pathSeparator) + 1) + "-" + name;
        saveMapper.writeKeyFile(fileName, prefixesJson.toString(), true);
        saveMapper.closeWriters();
        System.out.printf("please check the prefixes breakpoint in %s%s, it can be used for one more time " +
                "listing remaining files.\n", fileName, FileSaveMapper.ext);
    }

    JsonObject recordLister(ILister<E> lister) {
        JsonObject json = continuePrefixConf(lister);
        if (json != null) recordPrefixConfig(lister.getPrefix(), json);
        return json;
    }

    JsonObject recordListerByPrefix(String prefix) {
        JsonObject json = prefixesMap.get(prefix) == null ? null : JsonUtils.toJsonObject(prefixesMap.get(prefix));
        recordPrefixConfig(prefix, json);
        return json;
    }

    private void updatePrefixAndEnd(ILister<E> lister) {
        if (!lister.hasNext() && prefixAndEndedMap.containsKey(lister.getPrefix())) {
            if (lister.getEndPrefix() == null || "".equals(lister.getEndPrefix())) {
                prefixAndEndedMap.get(lister.getPrefix()).put("start", lister.currentEndKey());
            } else {
                prefixAndEndedMap.remove(lister.getPrefix());
            }
        }
    }

    /**
     * 执行列举操作，直到当前的 lister 列举结束，并使用 processor 对象执行处理过程
     * @param lister 已经初始化的 lister 对象
     * @param saver 用于列举结果持久化的文件对象
     * @param processor 用于资源处理的处理器对象
     * @throws IOException 列举出现错误或者持久化错误抛出的异常
     */
    public void export(ILister<E> lister, IResultOutput<W> saver, ILineProcess<T> processor) throws IOException {
        ITypeConvert<E, T> converter = getNewConverter();
        ITypeConvert<E, String> stringConverter = getNewStringConverter();
        List<T> convertedList;
        List<String> writeList;
        List<E> objects = lister.currents();
        int retry;
        if (objects.size() <= 0) updatePrefixAndEnd(lister);
        // 初始化的 lister 包含首次列举的结果列表，需要先取出，后续向前列举时会更新其结果列表
        while (objects.size() > 0 || lister.hasNext()) {
            recordLister(lister);
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
            while (true) {
                try {
                    updatePrefixAndEnd(lister);
                    lister.listForward(); // 要求 listForward 实现中先做 hashNext 判断，if (!hasNext) clear();
                    objects = lister.currents();
                    break;
                } catch (SuitsException e) {
                    System.out.println("list objects by prefix:" + lister.getPrefix() + " retrying...\n" + e.getMessage());
                    retry = HttpRespUtils.listExceptionWithRetry(e, retry);
                }
            }
        }
    }

    protected abstract IResultOutput<W> getNewResultSaver(String order) throws IOException;

    /**
     * 将 lister 对象放入线程池进行执行列举，如果 processor 不为空则同时执行 process 过程
     * @param lister 列举对象
     * @param order 当前列举对象集的起始序号
     */
    void listing(ILister<E> lister, int order) {
        // 持久化结果标识信息
        String newOrder = String.valueOf(order);
        IResultOutput<W> saver = null;
        ILineProcess<T> lineProcessor = null;
        try {
            // 多线程情况下不要直接使用传入的 processor，因为对其关闭会造成 clone 的对象无法进行结果持久化的写入
            if (processor != null) lineProcessor = processor.clone();
            saver = getNewResultSaver(newOrder);
            String record = "order " + newOrder + ": " + lister.getPrefix();
            export(lister, saver, lineProcessor);
            record += "\tsuccessfully done";
            System.out.println(record);
            removePrefixConfig(lister.getPrefix());
        } catch (Throwable e) {
            e.printStackTrace();
            System.out.println("order " + newOrder + ": " + lister.getPrefix() + "\t" + recordLister(lister));
        } finally {
            UniOrderUtils.returnOrder(order);
            if (saver != null) saver.closeWriters();
            if (lineProcessor != null) lineProcessor.closeResource();
            lister.close();
        }
    }

    protected abstract ILister<E> getLister(String prefix, String marker, String start, String end) throws SuitsException;

    ILister<E> generateLister(String prefix) throws SuitsException {
        int retry = retryTimes;
        Map<String, String> map;
        if (prefixesMap.containsKey(prefix) && prefixesMap.get(prefix) != null) map = prefixesMap.get(prefix);
        else map = new HashMap<>();
        String marker = map.get("marker");
        String start = map.get("start");
        String end = map.get("end");
        while (true) {
            try {
                return getLister(prefix, marker, start, end);
            } catch (SuitsException e) {
                System.out.println("generate lister by prefix:" + prefix + " retrying...\n" + e.getMessage());
                retry = HttpRespUtils.listExceptionWithRetry(e, retry);
            }
        }
    }

    private String computePoint(ILister<E> lister, boolean doFutureCheck) {
        boolean next;
        try {
            next = doFutureCheck ? lister.hasFutureNext() : lister.hasNext();
        } catch (SuitsException e) {
            next = lister.hasNext();
        }
        String startPrefix = lister.getPrefix();
        String point = null;
        if (next) {
            // 如果存在 next 且当前获取的最后一个对象文件名不为空，则可以根据最后一个对象的文件名计算后续的前缀字符
            String endKey = lister.currentEndKey();
            int prefixLen = startPrefix.length();
            if (endKey == null) {
                lister.setStraight(true);
            } else if (endKey.length() > prefixLen) {
                // 如果最后一个对象的文件名长度大于 prefixLen，则可以取出从当前前缀开始的下一个字符 point，用于和预定义前缀列表进行比较，确定
                // lister 的 endPrefix
                point = endKey.substring(prefixLen, prefixLen + 1);
                // 如果此时下一个字符比预定义的最后一个前缀大的话（如中文文件名的情况）说明后续根据预定义前缀再检索无意义，则直接返回即可
                if (point.compareTo(originPrefixList.get(originPrefixList.size() - 1)) > 0) {
                    point = null;
                    lister.setStraight(true);
                // 如果 point 比第一个预定义前缀小则设置 lister 的结束位置到第一个预定义前缀
                } else if (point.compareTo(originPrefixList.get(0)) < 0) {
                    point = startPoint;
                    lister.setEndPrefix(startPrefix + startPoint);
                } else {
                    insertIntoPrefixesMap(startPrefix + point, new HashMap<String, String>(){{
                        put("marker", lister.getMarker());
                    }});
                    lister.setEndPrefix(endKey);
                }
            } else {
                point = startPoint;
                // 无 next 时直接将 lister 的结束位置设置到第一个预定义前
                lister.setEndPrefix(startPrefix + startPoint);
            }
        } else {
            lister.setStraight(true);
//            lister.setEndPrefix(endKey);
        }
        return point;
    }

    private List<ILister<E>> getListerListByPrefixes(Stream<String> prefixesStream) {
        List<ILister<E>> nextLevelList = prefixesStream.map(prefix -> {
            try {
                return generateLister(prefix);
            } catch (SuitsException e) {
                System.out.println("generate lister failed by " + prefix + "\t" + prefixesMap.get(prefix));
                e.printStackTrace(); return null;
            }
        }).filter(generated -> {
            if (generated == null) return false;
            else if (generated.currents().size() > 0 || generated.hasNext()) return true;
            else {
                removePrefixConfig(generated.getPrefix());
                return false;
            }
        }).collect(Collectors.toList());
        if (nextLevelList.size() > 0) {
            ILister<E> lastLister = nextLevelList.get(nextLevelList.size() - 1);
            prefixAndEndedMap.put(lastLister.getPrefix(), new HashMap<>());
        }
        return nextLevelList;
    }

    private List<ILister<E>> filteredNextList(ILister<E> lister) {
        String point = computePoint(lister, true);
        int order = UniOrderUtils.getOrder();
        executorPool.execute(() -> listing(lister, order));
        if (point != null) {
            List<String> nextPrefixes = originPrefixList.stream()
                    .filter(prefix -> prefix.compareTo(point) >= 0 && checkPrefix(prefix))
                    .map(prefix -> prefix = lister.getPrefix() + prefix)
                    .peek(this::recordListerByPrefix)
                    .collect(Collectors.toList());
            List<ILister<E>> nextLevelList = getListerListByPrefixes(nextPrefixes.stream());
            Iterator<ILister<E>> it = nextLevelList.iterator();
            while (it.hasNext()) {
                ILister<E> eiLister = it.next();
                if(eiLister.canStraight()) {
                    int eOrder = UniOrderUtils.getOrder();
                    executorPool.execute(() -> listing(eiLister, eOrder));
                    it.remove();
                }
            }
            return nextLevelList;
        } else {
            return null;
        }
    }

    private List<ILister<E>> finalTaskToPool(List<ILister<E>> listerList) {
        executorPool = Executors.newFixedThreadPool(threads);
        while (listerList != null && listerList.size() > 0 && listerList.size() < threads) {
            listerList = listerList.parallelStream().map(lister -> {
                if (lister.canStraight()) {
                    int order = UniOrderUtils.getOrder();
                    executorPool.execute(() -> listing(lister, order));
                    return null;
                } else {
                    // 对非 canStraight 的列举对象进行下一级的检索，得到更深层次前缀的可并发列举对象
                    return filteredNextList(lister);
                }
            }).filter(Objects::nonNull).reduce((list1, list2) -> { list1.addAll(list2); return list1; }).orElse(null);
        }
        if (listerList != null && listerList.size() > 0) {
            listerList.parallelStream().forEach(lister -> {
                int order = UniOrderUtils.getOrder();
                executorPool.execute(() -> listing(lister, order));
            });
        }
        executorPool.shutdown();
        return listerList;
    }

    private List<String> checkListerInPool(List<ILister<E>> listerList) throws Exception {
        List<String> extremePrefixes = null;
        int cValue = threads / 10;
        int count = 0;
        boolean startCheck = false;
        int unfinished;
        while (!executorPool.isTerminated()) {
            try {
                // 3600 次延时为 1 小时间隔，判断一次线程池情况
                if (count >= 3600) {
                    count = 0;
                    Iterator<ILister<E>> iterator = listerList.iterator();
                    ILister<E> iLister;
                    while (iterator.hasNext()) {
                        iLister = iterator.next();
                        if(!iLister.hasNext()) iterator.remove();
                    }
                    unfinished = listerList.size();
                    if (unfinished < cValue) {
                        if (unfinished < 5) startCheck = true;
                        if (startCheck) {
                            System.out.println("to re-split prefixes...");
                            for (ILister<E> lister : listerList) {
                                if (lister.hasNext()) {
                                    String lastKey = lister.truncate();
                                    String prefix = lister.getPrefix();
                                    if (extremePrefixes == null) extremePrefixes = new ArrayList<>();
                                    extremePrefixes.add(prefix);
                                    recordListerByPrefix(prefix);
                                    insertIntoPrefixesMap(prefix, new HashMap<String, String>() {{ put("start", lastKey); }});
                                }
                            }
                            startCheck = false;
                        }
                        startCheck = true;
                        if (cValue > 3) cValue = cValue / 2;
                    }
                    System.out.printf("unfinished: %s, cValue: %s\n", unfinished, cValue);
                } else {
                    Thread.sleep(1000); // 延时 1s 并计次
                    count++;
                }
            } catch (InterruptedException ignored) {
                Thread.sleep(1000);
                count++;
            }
        }
        return extremePrefixes;
    }

    private void concurrentListing(List<ILister<E>> listerList) throws Exception {
        try {
            listerList.addAll(getListerListByPrefixes(prefixes.parallelStream()));
            listerList = finalTaskToPool(listerList);
            List<String> extremePrefixes = checkListerInPool(listerList);
            while (extremePrefixes != null && extremePrefixes.size() > 0) {
                executorPool = Executors.newFixedThreadPool(threads);
                listerList = getListerListByPrefixes(extremePrefixes.parallelStream()).parallelStream()
                        .map(this::filteredNextList).filter(Objects::nonNull)
                        .reduce((list1, list2) -> { list1.addAll(list2); return list1; }).orElse(null);
                if (listerList != null && listerList.size() > 0) {
                    listerList.parallelStream().forEach(lister -> {
                        int order = UniOrderUtils.getOrder();
                        executorPool.execute(() -> listing(lister, order));
                    });
                }
                executorPool.shutdown();
                extremePrefixes = checkListerInPool(listerList);
            }
            List<String> phraseLastPrefixes = new ArrayList<>();
            for (Map.Entry<String, Map<String, String>> stringMapEntry : prefixAndEndedMap.entrySet()) {
                String prefix = stringMapEntry.getKey().substring(0, stringMapEntry.getKey().length() - 1);
                phraseLastPrefixes.add(prefix);
                System.out.printf("prefix: %s, %s\n", prefix, stringMapEntry.getValue());
                insertIntoPrefixesMap(prefix, stringMapEntry.getValue());
            }
            for (String phraseLastPrefix : phraseLastPrefixes) recordListerByPrefix(phraseLastPrefix);
            listerList = getListerListByPrefixes(phraseLastPrefixes.parallelStream());
            threads = listerList.size();
            System.out.printf("threads: %s\n", threads);
            if (threads > 0) finalTaskToPool(listerList);
            while (!executorPool.isTerminated()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                    Thread.sleep(1000);
                }
            }
            System.out.println("finished.");
        } catch (Throwable e) {
            executorPool.shutdownNow();
            e.printStackTrace();
            if (listerList != null) {
                for (ILister<E> lister : listerList) {
                    if (lister.currents() != null) recordLister(lister);
                }
            }
        } finally {
            writeContinuedPrefixConfig(savePath, "prefixes");
        }
    }

    /**
     * 根据当前参数值创建多线程执行数据源导出工作
     */
    public void export() throws Exception {
        String info = "list objects from bucket: " + bucket + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " running...");
        List<ILister<E>> listerList = new ArrayList<>();
        ILister<E> startLister;
        if (prefixes == null || prefixes.size() == 0) {
            startLister = generateLister("");
            if (threads > 1) {
                String point = computePoint(startLister, false);
                if (point == null) {
                    threads = 1;
                } else {
                    prefixes = originPrefixList.stream()
                            .filter(prefix -> prefix.compareTo(point) >= 0 && checkPrefix(prefix))
                            .collect(Collectors.toList());
                    prefixRight = true;
                }
            }
            if (threads <= 1) {
                int order = UniOrderUtils.getOrder();
                listing(startLister, order);
                return;
            }
        } else {
            if (prefixLeft) {
                startLister = generateLister("");
                startLister.setEndPrefix(prefixes.get(0));
            } else {
                startLister = generateLister(prefixes.get(0));
                prefixes = prefixes.subList(1, prefixes.size());
            }
        }
        if (startLister != null && (startLister.currents().size() > 0 || startLister.hasNext())) listerList.add(startLister);
        for (String prefix : prefixes) recordListerByPrefix(prefix);
        concurrentListing(listerList);
    }
}
