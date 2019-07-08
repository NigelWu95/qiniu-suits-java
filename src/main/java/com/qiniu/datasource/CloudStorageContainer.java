package com.qiniu.datasource;

import com.google.gson.JsonObject;
import com.qiniu.common.QiniuException;
import com.qiniu.common.SuitsException;
import com.qiniu.entry.CommonParams;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.IResultOutput;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.*;
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
    protected AtomicBoolean lastUpdated;
    protected ILineProcess<T> processor; // 定义的资源处理器
    protected List<String> originPrefixList = new ArrayList<>();
    public static String startPoint;

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
    protected boolean checkPrefix(String prefix) {
        if (prefix == null || "".equals(prefix)) return false;
        if (antiPrefixes == null) antiPrefixes = new ArrayList<>();
        for (String antiPrefix : antiPrefixes) {
            if (prefix.startsWith(antiPrefix)) return false;
        }
        return true;
    }

    protected abstract ITypeConvert<E, T> getNewConverter();

    protected abstract ITypeConvert<E, String> getNewStringConverter() throws IOException;

    protected int listExceptionWithRetry(SuitsException e, int retry) throws SuitsException {
        if (e.getStatusCode() == 401 && e.getMessage().contains("date offset error")) {
            retry--;
        } else if (e.getStatusCode() == 429) {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException interruptEx) {
                e.setError(e.getMessage() + "\t" + interruptEx.getMessage());
                throw e;
            }
        } else if (HttpRespUtils.checkStatusCode(e.getStatusCode()) < 0 || (retry <= 0 && e.getStatusCode() >= 500)) {
            throw e;
        } else {
            retry--;
        }
        return retry;
    }

    protected JsonObject recordLister(ILister<E> lister) {
        JsonObject json = ListingUtils.continuePrefixConf(lister);
        if (json != null) ListingUtils.recordPrefixConfig(lister.getPrefix(), json);
        return json;
    }

    protected JsonObject recordListerByPrefix(String prefix) {
        JsonObject json = prefixesMap.get(prefix) == null ? null : JsonUtils.toJsonObject(prefixesMap.get(prefix));
        ListingUtils.recordPrefixConfig(prefix, json);
        return json;
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
                    lister.listForward(); // 要求 listForward 实现中先做 hashNext 判断，if (!hasNext) clear();
                    objects = lister.currents();
                    break;
                } catch (SuitsException e) {
                    System.out.println("list objects by prefix:" + lister.getPrefix() + " retrying...\n" + e.getMessage());
                    retry = listExceptionWithRetry(e, retry);
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
    protected void listing(ILister<E> lister, int order) {
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
            ListingUtils.removePrefixConfig(lister.getPrefix());
        } catch (Exception e) {
            System.out.println("order " + newOrder + ": " + lister.getPrefix() + "\t" + recordLister(lister));
            e.printStackTrace();
        } finally {
            UniOrderUtils.returnOrder(order);
            if (saver != null) saver.closeWriters();
            if (lineProcessor != null) lineProcessor.closeResource();
            lister.close();
        }
    }

    protected abstract ILister<E> getLister(String prefix, String marker, String start, String end) throws SuitsException;

    protected ILister<E> generateLister(String prefix) throws SuitsException {
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
                retry = listExceptionWithRetry(e, retry);
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
        return prefixesStream.map(prefix -> {
            try {
                return generateLister(prefix);
            } catch (SuitsException e) {
                System.out.println("generate lister failed by " + prefix + "\t" + recordListerByPrefix(prefix));
                e.printStackTrace(); return null;
            }
        }).filter(generated -> {
            if (generated == null) return false;
            else if (generated.currents().size() > 0 || generated.hasNext()) return true;
            else {
                ListingUtils.removePrefixConfig(generated.getPrefix());
                return false;
            }
        }).collect(Collectors.toList());
    }

    private List<ILister<E>> filteredNextList(ILister<E> lister) {
        String point = computePoint(lister, true);
        List<ILister<E>> nextLevelList = new ArrayList<ILister<E>>(){{ add(lister); }};
        if (point != null) {
            List<String> nextPrefixes = originPrefixList.stream()
                    .filter(prefix -> prefix.compareTo(point) >= 0 && checkPrefix(prefix))
                    .map(prefix -> prefix = lister.getPrefix() + prefix)
                    .peek(this::recordListerByPrefix)
                    .collect(Collectors.toList());
            nextLevelList.addAll(getListerListByPrefixes(nextPrefixes.stream()));
        }
        Iterator<ILister<E>> it = nextLevelList.iterator();
        int size = nextLevelList.size();
        // 为了更优的列举性能，考虑将每个 prefix 下一级迭代过程中产生的部分 lister 先执行，因为产生的下级列举对象本身是按前
        // 缀有序的，故保留最后一个不做执行，用于返回到汇总的列表中判断最后一个列举对象是否需要更新
        while (it.hasNext() && size > 1) {
            size--;
            ILister<E> eiLister = it.next();
            if(eiLister.canStraight()) {
                int order = UniOrderUtils.getOrder();
                executorPool.execute(() -> listing(eiLister, order));
                it.remove();
            }
        }
        return nextLevelList;
    }

    private List<ILister<E>> computeNextAndFilterList(List<ILister<E>> listerList, String lastPrefix) {
        if (!lastUpdated.get()) {
            ILister<E> lastLister =
            listerList.stream().max(Comparator.comparing(ILister::getPrefix)).get();
//            System.out.println("lastLister: " + lastLister.getPrefix() + "\t" + lastLister.currents().size() + "\t" + lastLister.hasNext());
            // 得到计算后的最后一个列举对象，如果不存在 next 则说明该对象是下一级的末尾（最靠近结束位置）列举对象，更新其末尾设置
            if (!lastLister.hasNext()) {
                // 全局结尾则设置前缀为空，否则设置前缀为起始值
                lastLister.setPrefix(lastPrefix);
                lastLister.updateMarkerBy(lastLister.currentLast());
                lastLister.setStraight(true);
                lastUpdated.set(true);
            }
        }
        return listerList.parallelStream().map(lister -> {
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

    private void prefixListing(List<ILister<E>> listerList, String lastPrefix) {
//        while (listerList != null && listerList.size() > 0) {
//            listerList = computeNextAndFilterList(listerList, lastPrefix);
//        }
        while (listerList != null && listerList.size() > 0 && listerList.size() < threads) {
            listerList = computeNextAndFilterList(listerList, lastPrefix);
        }
        if (listerList != null && listerList.size() > 0) {
            // 如果末尾的 lister 尚未更新末尾设置则需要对此时的最后一个列举对象进行末尾设置的更新
            if (!lastUpdated.get()) {
                ILister<E> lastLister = listerList.stream().max(Comparator.comparing(ILister::getPrefix)).get();
                lastLister.setPrefix(lastPrefix);
                if (!lastLister.hasNext()) lastLister.updateMarkerBy(lastLister.currentLast());
            }
            listerList.parallelStream().forEach(lister -> {
                int order = UniOrderUtils.getOrder();
                executorPool.execute(() -> listing(lister, order));
            });
        }
    }

    private void checkListerInPool(List<ILister<E>> listerList, String lastPrefix, long startTime) {
        long interval = (System.currentTimeMillis() - startTime) / 1000;
        if (interval >= 3600) {
            List<String> extremePrefixes = null;
            for (ILister<E> lister : listerList) {
                if (lister.hasNext()) {
                    String endKey = lister.currentEndKey();
                    String prefix = lister.getPrefix();
                    lister.setEndPrefix(endKey);
                    if (extremePrefixes == null) extremePrefixes = new ArrayList<>();
                    extremePrefixes.add(prefix);
                    insertIntoPrefixesMap(prefix, new HashMap<String, String>() {{ put("start", endKey); }});
                }
            }
            if (extremePrefixes != null && extremePrefixes.size() > 0) {
                listerList = getListerListByPrefixes(extremePrefixes.parallelStream());
                prefixListing(listerList, lastPrefix);
            }
        }
    }

    private void concurrentListing(ILister<E> startLister, String fPoint, String lastPrefix, String info) throws IOException {
        lastUpdated = new AtomicBoolean(false);
        executorPool = Executors.newFixedThreadPool(threads);
        List<ILister<E>> listerList = null;
        try {
            prefixes = prefixes.stream()
                    .filter(prefix -> prefix.compareTo(fPoint) >= 0 && checkPrefix(prefix))
                    .peek(this::recordListerByPrefix)
                    .collect(Collectors.toList());
            listerList = getListerListByPrefixes(prefixes.parallelStream());
            listerList.add(startLister);
            prefixListing(listerList, lastPrefix);
            executorPool.shutdown();
            int unfinished = listerList.size();
            int cValue = threads / 4;
            long startTime;
            while (!executorPool.isTerminated()) {
                try {
                    if (unfinished > 0 && unfinished < cValue) {
                        cValue = cValue / 2;
                        startTime = System.currentTimeMillis();
                        checkListerInPool(listerList, lastPrefix, startTime);
                    } else {
                        unfinished = 0;
                        for (ILister<E> lister : listerList) if (lister.hasNext()) unfinished++;
                    }
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                    Thread.sleep(1000);
                }
            }
            System.out.println(info + " finished");
        } catch (Throwable e) {
            executorPool.shutdownNow();
            e.printStackTrace();
            if (listerList != null) {
                for (ILister<E> lister : listerList) {
                    if (lister.currents() != null) recordLister(lister);
                }
            }
            System.out.println("lastUpdated: " + lastUpdated.get());
        } finally {
            ListingUtils.writeContinuedPrefixConfig(savePath, "prefixes");
        }
    }

    /**
     * 根据当前参数值创建多线程执行数据源导出工作
     */
    public void export() throws Exception {
        String info = "list objects from bucket: " + bucket + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " running...");
        ILister<E> startLister;
        String point = "";
        String lastPrefix = "";
        if (prefixes == null || prefixes.size() == 0) {
            startLister = generateLister("");
            if (threads > 1) point = computePoint(startLister, false);
            if (point == null || "".equals(point)) {
                int order = UniOrderUtils.getOrder();
                listing(startLister, order);
                return;
            } else {
                prefixes = originPrefixList;
            }
        } else {
            if (prefixLeft) {
                startLister = generateLister("");
                startLister.setEndPrefix(prefixes.get(0));
            } else {
                startLister = generateLister(prefixes.get(0));
                prefixes = prefixes.subList(1, prefixes.size());
            }
            lastPrefix = prefixRight ? "" : prefixes.get(prefixes.size() - 1);
        }
        concurrentListing(startLister, point, lastPrefix, info);
    }
}
