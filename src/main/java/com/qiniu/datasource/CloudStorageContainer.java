package com.qiniu.datasource;

import com.google.gson.JsonObject;
import com.qiniu.common.JsonRecorder;
import com.qiniu.common.QiniuException;
import com.qiniu.common.SuitsException;
import com.qiniu.interfaces.IDataSource;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.interfaces.ILister;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.interfaces.IResultOutput;
import com.qiniu.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.qiniu.entry.CommonParams.lineFormats;

public abstract class CloudStorageContainer<E, W, T> implements IDataSource<ILister<E>, IResultOutput<W>, T> {

    static final Logger rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    static final Logger errorLogger = LoggerFactory.getLogger("error");
    static final File errorLogFile = new File("qsuits.error");
    private static final Logger infoLogger = LoggerFactory.getLogger("info");
    private static final File infoLogFile = new File("qsuits.info");
    private static final Logger procedureLogger = LoggerFactory.getLogger("procedure");
    private static final File procedureLogFile = new File("procedure.log");

    protected String bucket;
    protected List<String> antiPrefixes;
    protected boolean hasAntiPrefixes = false;
    protected Map<String, Map<String, String>> prefixesMap;
    protected List<String> prefixes;
    protected boolean prefixLeft;
    protected boolean prefixRight;
    protected int unitLen;
    protected int threads;
    protected int retryTimes = 5;
    protected boolean saveTotal;
    protected String savePath;
    protected String saveFormat;
    protected String saveSeparator;
    protected List<String> rmFields;
    protected Map<String, String> indexMap;
    protected List<String> fields;
    protected ExecutorService executorPool; // 线程池
    protected ILineProcess<T> processor; // 定义的资源处理器
    protected List<String> originPrefixList = new ArrayList<>();
    static String firstPoint;
    private String lastPoint;
    private ConcurrentMap<String, Map<String, String>> prefixAndEndedMap = new ConcurrentHashMap<>(100);
    private ConcurrentMap<String, IResultOutput<W>> saverMap = new ConcurrentHashMap<>(threads);
    private ConcurrentMap<String, ILineProcess<T>> processorMap = new ConcurrentHashMap<>(threads);

    public CloudStorageContainer(String bucket, Map<String, Map<String, String>> prefixesMap, List<String> antiPrefixes,
                                 boolean prefixLeft, boolean prefixRight, Map<String, String> indexMap, List<String> fields,
                                 int unitLen, int threads) throws IOException {
        this.bucket = bucket;
        this.prefixLeft = prefixLeft;
        this.prefixRight = prefixRight;
        // 先设置 antiPrefixes 后再设置 prefixes，因为可能需要从 prefixes 中去除 antiPrefixes 含有的元素
        this.antiPrefixes = antiPrefixes;
        if (antiPrefixes != null && antiPrefixes.size() > 0) hasAntiPrefixes = true;
        setPrefixesAndMap(prefixesMap);
        this.unitLen = unitLen;
        this.threads = threads;
        // default save parameters
        this.saveTotal = true; // 默认全记录保存
        this.savePath = "result";
        this.saveFormat = "tab";
        this.saveSeparator = "\t";
        setIndexMapWithDefault(indexMap);
        if (fields == null || fields.size() == 0) {
            this.fields = ConvertingUtils.getOrderedFields(this.indexMap, rmFields);
        }
        else this.fields = fields;
        // 由于目前指定包含 "|" 字符的前缀列举会导致超时，因此先将该字符及其 ASCII 顺序之前的 "{" 和之后的（"|}~"）统一去掉，从而优化列举的超
        // 时问题，简化前缀参数的设置，也避免为了兼容该字符去修改代码算法
        originPrefixList.addAll(Arrays.asList(("!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMN").split("")));
        originPrefixList.addAll(Arrays.asList(("OPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz").split("")));
        firstPoint = originPrefixList.get(0);
        lastPoint = originPrefixList.get(originPrefixList.size() - 1);
    }

    // 不调用则各参数使用默认值
    public void setSaveOptions(boolean saveTotal, String savePath, String format, String separator, List<String> rmFields)
            throws IOException {
        this.saveTotal = saveTotal;
        this.savePath = savePath;
        this.saveFormat = format;
        if (!lineFormats.contains(saveFormat)) throw new IOException("please check your format for map to string.");
        this.saveSeparator = separator;
        this.rmFields = rmFields;
        if (rmFields != null && rmFields.size() > 0) {
            this.fields = ConvertingUtils.getFields(fields, rmFields);
        }
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes < 1 ? 5 : retryTimes;
    }

    private void setIndexMapWithDefault(Map<String, String> indexMap) throws IOException {
        if (indexMap == null || indexMap.size() == 0) {
            if (this.indexMap == null) this.indexMap = new HashMap<>();
            for (String fileInfoField : ConvertingUtils.defaultFileFields) {
                this.indexMap.put(fileInfoField, fileInfoField);
            }
        } else {
            for (String s : indexMap.keySet()) {
                if (s == null || "".equals(s)) throw new IOException("the index can not be empty in " + indexMap);
            }
            this.indexMap = indexMap;
        }
    }

    public void setProcessor(ILineProcess<T> processor) {
        this.processor = processor;
    }

    private void setPrefixesAndMap(Map<String, Map<String, String>> prefixesMap) throws IOException {
        if (prefixesMap == null || prefixesMap.size() <= 0) {
            this.prefixesMap = new HashMap<>();
            prefixLeft = true;
            prefixRight = true;
            if (hasAntiPrefixes) prefixes = originPrefixList.stream().sorted().collect(Collectors.toList());
        } else {
            if (prefixesMap.containsKey(null)) throw new IOException("");
            this.prefixesMap = new HashMap<>(prefixesMap);
            prefixes = prefixesMap.keySet().stream().sorted().collect(Collectors.toList());
            int size = prefixes.size();
            Iterator<String> iterator = prefixes.iterator();
            String temp = iterator.next();
            Map<String, String> value = prefixesMap.get(temp);
            String start = null;
            String end = null;
            String marker = null;
            if (temp.equals("") && !iterator.hasNext()) {
                if (value != null && value.size() > 0) {
                    start = "".equals(value.get("start")) ? null : value.get("start");
                    end = "".equals(value.get("end")) ? null : value.get("end");
                    marker = "".equals(value.get("marker")) ? null : value.get("marker");
                }
                if (start == null && end == null && marker == null) throw new IOException("prefixes can not only be empty string(\"\")");
            }
            while (iterator.hasNext() && size > 0) {
                size--;
                String prefix = iterator.next();
                if (prefix.startsWith(temp)) {
                    end = value == null ? null : value.get("end");
                    if (end == null || "".equals(end)) {
                        iterator.remove();
                        this.prefixesMap.remove(prefix);
                    } else if (end.compareTo(prefix) >= 0) {
                        throw new IOException(temp + "'s end can not be more larger than " + prefix + " in " + prefixesMap);
                    }
                } else {
                    temp = prefix;
                    value = prefixesMap.get(temp);
                }
            }
        }
        if (hasAntiPrefixes && prefixes != null && prefixes.size() > 0) {
            String lastAntiPrefix = antiPrefixes.stream().max(Comparator.naturalOrder()).orElse(null);
            if (prefixRight && lastAntiPrefix != null && lastAntiPrefix.compareTo(prefixes.get(prefixes.size() - 1)) <= 0) {
                throw new IOException("max anti-prefix can not be same as or more larger than max prefix.");
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
        if (prefix == null) return false;
        if (hasAntiPrefixes) {
            for (String antiPrefix : antiPrefixes) {
                if (prefix.startsWith(antiPrefix)) return false;
            }
            return true;
        } else {
            return true;
        }
    }

    protected abstract ITypeConvert<E, T> getNewConverter();

    protected abstract ITypeConvert<E, String> getNewStringConverter() throws IOException;

    private JsonRecorder recorder = new JsonRecorder();

    void recordListerByPrefix(String prefix) {
        JsonObject json = prefixesMap.get(prefix) == null ? null : JsonUtils.toJsonObject(prefixesMap.get(prefix));
        try { FileUtils.createIfNotExists(procedureLogFile); } catch (IOException ignored) {}
        procedureLogger.info(recorder.put(prefix, json));
    }

    /**
     * 执行列举操作，直到当前的 lister 列举结束，并使用 processor 对象执行处理过程
     * @param lister 已经初始化的 lister 对象
     * @param saver 用于列举结果持久化的文件对象
     * @param processor 用于资源处理的处理器对象
     * @throws IOException 列举出现错误或者持久化错误抛出的异常
     */
    public void export(ILister<E> lister, IResultOutput<W> saver, ILineProcess<T> processor) throws Exception {
        ITypeConvert<E, T> converter = getNewConverter();
        ITypeConvert<E, String> stringConverter = null;
        if (saveTotal) {
            stringConverter = getNewStringConverter();
            saver.preAddWriter("failed");
        }
        List<T> convertedList;
        List<String> writeList;
        List<E> objects = lister.currents();
        boolean hasNext = lister.hasNext();
        int retry;
        Map<String, String> map = prefixAndEndedMap.get(lister.getPrefix());
        // 初始化的 lister 包含首次列举的结果列表，需要先取出，后续向前列举时会更新其结果列表
        while (objects.size() > 0 || hasNext) {
            if (LocalDateTime.now(DatetimeUtils.clock_Default).isAfter(pauseDateTime)) {
                synchronized (object) {
                    object.wait();
                }
            }
            if (stringConverter != null) {
                writeList = stringConverter.convertToVList(objects);
                if (writeList.size() > 0) saver.writeSuccess(String.join("\n", writeList), false);
                if (stringConverter.errorSize() > 0) saver.writeToKey("failed", stringConverter.errorLines(), false);
            }
            if (processor != null) {
                convertedList = converter.convertToVList(objects);
                if (converter.errorSize() > 0) saver.writeError(converter.errorLines(), false);
                // 如果抛出异常需要检测下异常是否是可继续的异常，如果是程序可继续的异常，忽略当前异常保持数据源读取过程继续进行
                try {
                    processor.processLine(convertedList);
                } catch (QiniuException e) {
                    if (HttpRespUtils.checkException(e, 2) < -1) throw e;
                    errorLogger.error("process objects: {}", lister.getPrefix(), e);
                    if (e.response != null) e.response.close();
                }
            }
            if (hasNext) {
                JsonObject json = recorder.getOrDefault(lister.getPrefix(), new JsonObject());
                json.addProperty("marker", lister.getMarker());
                json.addProperty("end", lister.getEndPrefix());
                try { FileUtils.createIfNotExists(procedureLogFile); } catch (IOException ignored) {}
                procedureLogger.info(recorder.put(lister.getPrefix(), json));
            }
            if (map != null) map.put("start", lister.currentEndKey());
//            objects.clear(); 上次其实不能做 clear，会导致 lister 中的列表被清空
            retry = retryTimes;
            while (true) {
                try {
                    lister.listForward(); // 要求 listForward 实现中先做 hashNext 判断，if (!hasNext) 置空;
                    objects = lister.currents();
                    break;
                } catch (SuitsException e) {
                    retry = HttpRespUtils.listExceptionWithRetry(e, retry);
                    try {FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
                    errorLogger.error("list objects by prefix:{} retrying...", lister.getPrefix(), e);
                }
            }
            hasNext = lister.hasNext();
        }
    }

    protected abstract IResultOutput<W> getNewResultSaver(String order) throws IOException;

    /**
     * 将 lister 对象放入线程池进行执行列举，如果 processor 不为空则同时执行 process 过程
     * @param lister 列举对象
     */
    void listing(ILister<E> lister) {
        // 持久化结果标识信息
        int order = UniOrderUtils.getOrder();
        String orderStr = String.valueOf(order);
        IResultOutput<W> saver = null;
        ILineProcess<T> lineProcessor = null;
        try {
            saver = getNewResultSaver(orderStr);
            saverMap.put(orderStr, saver);
            // 多线程情况下不要直接使用传入的 processor，因为对其关闭会造成 clone 的对象无法进行结果持久化的写入
            if (processor != null) {
                lineProcessor = processor.clone();
                processorMap.put(orderStr, lineProcessor);
            }
            export(lister, saver, lineProcessor);
            recorder.remove(lister.getPrefix()); // 只有 export 成功情况下才移除 record
        } catch (QiniuException e) {
            try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
            errorLogger.error("{}: {}, {}", lister.getPrefix(), recorder.getJson(lister.getPrefix()), e.error(), e);
            if (e.response != null) e.response.close();
        } catch (Throwable e) {
            try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
            errorLogger.error("{}: {}", lister.getPrefix(), recorder.getJson(lister.getPrefix()), e);
        } finally {
            try { FileUtils.createIfNotExists(infoLogFile); } catch (IOException ignored) {}
            infoLogger.info("{}\t{}\t{}", orderStr, lister.getPrefix(), lister.count());
            if (saver != null) {
                saver.closeWriters();
                saver = null; // let gc work
            }
            saverMap.remove(orderStr);
            if (lineProcessor != null) {
                lineProcessor.closeResource();
                lineProcessor = null;
                // 实际上 processorMap 记录的 key 和 saverMap 的是一致的，只要 saverMap 中做了移除说明已经正常处理完任务，最后直接根据
                // saverMap 的 keySet 来 check 即可
            }
            UniOrderUtils.returnOrder(order); // 最好执行完 close 再归还 order，避免上个文件描述符没有被使用，order 又被使用
            lister.close();
        }
    }

    protected abstract ILister<E> getLister(String prefix, String marker, String start, String end) throws SuitsException;

    ILister<E> generateLister(String prefix) throws SuitsException {
        int retry = retryTimes;
        Map<String, String> map = prefixesMap.get(prefix);
        String marker;
        String start;
        String end;
        if (map == null) {
            marker = start = end = null;
        } else {
            marker = map.get("marker");
            start = map.get("start");
            end = map.get("end");
        }
        while (true) {
            try {
                return getLister(prefix, marker, start, end);
            } catch (SuitsException e) {
                retry = HttpRespUtils.listExceptionWithRetry(e, retry);
                try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
                errorLogger.error("generate lister by prefix:{} retrying...", prefix, e);
            }
        }
    }

    private List<String> moreValidPrefixes(ILister<E> lister, boolean doFutureCheck) {
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
            if (endKey != null) {
                if (endKey.length() > prefixLen) {
                    // 如果最后一个对象的文件名长度大于 prefixLen，则可以取出从当前前缀开始的下一个字符 point，用于和预定义前缀列表进行比较，
                    // 确定 lister 的 endPrefix
                    point = endKey.substring(prefixLen, prefixLen + 1);
                    // 如果此时下一个字符比预定义的最后一个前缀大的话（如中文文件名的情况）说明后续根据预定义前缀再检索无意义，则直接返回即可
                    if (point.compareTo(lastPoint) > 0) {
                        point = null;
                    // 如果 point 比第一个预定义前缀小则设置 lister 的结束位置到第一个预定义前缀
                    } else if (point.compareTo(firstPoint) < 0) {
                        point = firstPoint;
                        lister.setEndPrefix(startPrefix + firstPoint);
                        lister.setLimit(1);
                    } else {
                        insertIntoPrefixesMap(startPrefix + point, new HashMap<String, String>(){{
                            put("marker", lister.getMarker());
                        }});
                        lister.setEndPrefix(endKey);
                    }
                } else {
                    point = firstPoint;
                    // 无 next 时直接将 lister 的结束位置设置到第一个预定义前
                    lister.setEndPrefix(startPrefix + firstPoint);
                    lister.setLimit(1);
                }
            } else {
                return moreValidPrefixes(lister, true);
            }
        }
        if (point != null) {
            String finalPoint = point;
            return originPrefixList.stream().filter(prefix -> prefix.compareTo(finalPoint) >= 0)
                    .map(prefix -> lister.getPrefix() + prefix).filter(this::checkPrefix)
                    .peek(this::recordListerByPrefix).collect(Collectors.toList());
        } else {
            return null;
        }
    }

    private List<ILister<E>> filteredListerByPrefixes(Stream<String> prefixesStream) {
        List<ILister<E>> prefixesLister = prefixesStream.map(prefix -> {
            try {
                return generateLister(prefix);
            } catch (SuitsException e) {
                try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
                errorLogger.error("generate lister failed by {}\t{}", prefix, prefixesMap.get(prefix), e);
                return null;
            }
        }).filter(generated -> {
            if (generated == null) return false;
            else if (generated.currents().size() > 0 || generated.hasNext()) return true;
            else {
                recorder.remove(generated.getPrefix());
                generated.close();
                return false;
            }
        }).collect(Collectors.toList());
        if (prefixesLister.size() > 0) {
            ILister<E> lastLister = prefixesLister.stream().max(Comparator.comparing(ILister::getPrefix)).get();
            Map<String, String> map = prefixesMap.get(lastLister.getPrefix());
            if (map == null) {
                prefixAndEndedMap.put(lastLister.getPrefix(), new HashMap<>());
            } else if (!map.containsKey("remove")) {
                prefixAndEndedMap.put(lastLister.getPrefix(), map);
            }
        }
        Iterator<ILister<E>> it = prefixesLister.iterator();
        while (it.hasNext()) {
            ILister<E> nLister = it.next();
            if(!nLister.hasNext() || (nLister.getEndPrefix() != null && !"".equals(nLister.getEndPrefix()))) {
                executorPool.execute(() -> listing(nLister));
                it.remove();
            }
        }
        return prefixesLister;
    }

    private void processNodeLister(ILister<E> lister) {
        if (lister.currents().size() > 0 || lister.hasNext()) {
            executorPool.execute(() -> listing(lister));
        } else {
            recorder.remove(lister.getPrefix());
            lister.close();
        }
    }

    private List<ILister<E>> computeToNextLevel(List<ILister<E>> listerList) {
        return listerList.parallelStream().map(lister -> {
            List<String> nextPrefixes = moreValidPrefixes(lister, true);
            processNodeLister(lister);
            if (nextPrefixes != null) {
                return filteredListerByPrefixes(nextPrefixes.stream());
            } else {
                return null;
            }
        }).filter(Objects::nonNull).reduce((list1, list2) -> { list1.addAll(list2); return list1; }).orElse(null);
    }

    private List<String> checkListerInPool(List<ILister<E>> listerList, int cValue, int tiny) {
        List<String> extremePrefixes = null;
        int count = 0;
        ILister<E> iLister;
        Iterator<ILister<E>> iterator;
        String prefix;
        String nextMarker;
        String start;
        Map<String, String> endMap;
        Map<String, String> prefixMap;
        while (!executorPool.isTerminated()) {
            if (count >= 1800) {
                iterator = listerList.iterator();
                while (iterator.hasNext()) {
                    iLister = iterator.next();
                    if(!iLister.hasNext()) iterator.remove();
                }
                if (listerList.size() > 0 && listerList.size() <= tiny) {
                    rootLogger.info("unfinished: {}, cValue: {}, to re-split prefixes...\n", listerList.size(), cValue);
                    for (ILister<E> lister : listerList) {
                        // lister 的 prefix 为 final 对象，不能因为 truncate 的操作之后被修改
                        prefix = lister.getPrefix();
                        nextMarker = lister.truncate();
                        // 防止 truncate 过程中原来的线程中丢失了 prefixAndEndedMap 的操作，这里再判断一次
                        endMap = prefixAndEndedMap.get(prefix);
                        prefixMap = new HashMap<>();
                        if (endMap == null) {
                            prefixMap.put("remove", "remove");
                        } else {
                            start = lister.currentEndKey();
                            if (start != null) endMap.put("start", start);
                        }
                        rootLogger.info("prefix: {}, nextMarker: {}, endMap: {}\n", prefix, nextMarker, endMap);
                        // 如果 truncate 时的 nextMarker 已经为空说明已经列举完成了
                        if (nextMarker == null || nextMarker.isEmpty()) continue;
                        if (extremePrefixes == null) extremePrefixes = new ArrayList<>();
                        extremePrefixes.add(prefix);
                        prefixMap.put("marker", nextMarker);
                        insertIntoPrefixesMap(prefix, prefixMap);
                    }
                } else if (listerList.size() <= cValue) {
                    count = 1200;
                } else {
                    count = 0;
                }
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                int i = 0;
                while (i < 1000) i++;
            }
            count++;
        }
        return extremePrefixes;
    }

    private List<String> lastEndedPrefixes() {
        List<String> phraseLastPrefixes = new ArrayList<>(prefixAndEndedMap.keySet());
        phraseLastPrefixes.sort(Comparator.reverseOrder());
        String previousPrefix;
        Map<String, String> prefixMap;
        String start;
        Set<String> startPrefixes = prefixes == null ? new HashSet<>() : new HashSet<>(prefixes);
        for (String prefix : phraseLastPrefixes) {
            prefixMap = prefixAndEndedMap.get(prefix);
            rootLogger.info("prefix: {}, endMap: {}", prefix, prefixMap);
            if (prefixMap == null || prefixMap.size() == 0) {
                prefixAndEndedMap.remove(prefix);
                continue;
            }
//            recorder.remove(prefix);
            start = prefixMap.get("start");
            // 由于优先使用 marker 原则，为了 start 生效则将可能的 marker 删除
            if (start != null && !"".equals(start)) {
                prefixMap.remove("marker");
            } else {
                prefixAndEndedMap.remove(prefix);
                continue;
            }
            if (startPrefixes.contains(prefix)) {
                if (prefixRight) prefixAndEndedMap.put("", prefixMap);
                prefixAndEndedMap.remove(prefix);
            } else {
                previousPrefix = prefix.substring(0, prefix.length() - 1);
                prefixAndEndedMap.put(previousPrefix, prefixMap);
                prefixAndEndedMap.remove(prefix);
            }
        }
        prefixesMap.putAll(prefixAndEndedMap);
        phraseLastPrefixes = prefixAndEndedMap.keySet().stream().sorted().collect(Collectors.toList());
        for (String phraseLastPrefix : phraseLastPrefixes) recordListerByPrefix(phraseLastPrefix);
        return phraseLastPrefixes;
    }

    private void waitAndTailListing(List<ILister<E>> listerList) {
        int cValue = threads < 10 ? 3 : threads / 2;
        int tiny = threads >= 300 ? 30 : threads >= 200 ? 20 : threads >= 100 ? 10 : threads >= 50 ? threads / 10 :
                threads >= 10 ? 3 : 1;
        List<String> extremePrefixes = checkListerInPool(listerList, cValue, tiny);
        while (extremePrefixes != null && extremePrefixes.size() > 0) {
            for (String prefix : extremePrefixes) recordListerByPrefix(prefix);
            executorPool = Executors.newFixedThreadPool(threads);
            listerList = filteredListerByPrefixes(extremePrefixes.parallelStream());
            while (listerList != null && listerList.size() > 0 && listerList.size() <= threads) {
                prefixesMap.clear();
                listerList = computeToNextLevel(listerList);
            }
            if (listerList != null && listerList.size() > 0) {
                listerList.parallelStream().forEach(lister -> executorPool.execute(() -> listing(lister)));
            }
            executorPool.shutdown();
            extremePrefixes = checkListerInPool(listerList, cValue, tiny);
        }
        List<String> phraseLastPrefixes = lastEndedPrefixes();
        if (phraseLastPrefixes.size() > 0) {
            executorPool = Executors.newFixedThreadPool(phraseLastPrefixes.size());
            listerList = filteredListerByPrefixes(phraseLastPrefixes.parallelStream());
            listerList.parallelStream().forEach(lister -> executorPool.execute(() -> listing(lister)));
            executorPool.shutdown();
        }
        while (!executorPool.isTerminated()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                int i = 0;
                while (i < 1000) i++;
            }
        }
    }

    void endAction() throws IOException {
        ILineProcess<T> processor;
        for (Map.Entry<String, IResultOutput<W>> saverEntry : saverMap.entrySet()) {
            saverEntry.getValue().closeWriters();
            processor = processorMap.get(saverEntry.getKey());
            if (processor != null) processor.closeResource();
        }
        String record = recorder.toString();
        if (recorder.size() > 0) {
            FileSaveMapper.ext = ".json";
            String path = new File(savePath).getCanonicalPath();
            FileSaveMapper saveMapper = new FileSaveMapper(new File(path).getParent());
//        if (path.endsWith("/")) path = path.substring(0, path.length() - 1);
            String fileName = path.substring(path.lastIndexOf(FileUtils.pathSeparator) + 1) + "-prefixes";
            saveMapper.addWriter(fileName);
            saveMapper.writeToKey(fileName, record, true);
            saveMapper.closeWriters();
            rootLogger.info("please check the prefixes breakpoint in {}{}, it can be used for one more time listing remained objects.",
                    fileName, FileSaveMapper.ext);
        }
        procedureLogger.info(record);
    }

    void showdownHook() {
        SignalHandler handler = signal -> {
            try {
//                executorPool.shutdownNow();
                pauseDateTime = LocalDateTime.MIN; // 使用该语句使线程池中的任务停滞效果可能比 shutdownNow() 要好
                endAction();
            } catch (IOException e) {
                rootLogger.error("showdown", e);
            }
            System.exit(0);
        };
        try { // 设置 INT 信号 (Ctrl + C 中断执行) 交给指定的信号处理器处理，废掉系统自带的功能
            Signal.handle(new Signal("INT"), handler); } catch (Exception ignored) {}
        try { Signal.handle(new Signal("TERM"), handler); } catch (Exception ignored) {}
        try { Signal.handle(new Signal("USR1"), handler); } catch (Exception ignored) {}
        try { Signal.handle(new Signal("USR2"), handler); } catch (Exception ignored) {}
        // 以下几种信号实际上不能被处理
//        try { Signal.handle(new Signal("QUIT"), handler); } catch (Exception ignored) {}
//        try { Signal.handle(new Signal("KILL"), handler); } catch (Exception ignored) {}
//        try { Signal.handle(new Signal("STOP"), handler); } catch (Exception ignored) {}
    }

    /**
     * 根据当前参数值创建多线程执行数据源导出工作
     */
    public void export() throws Exception {
        String info = processor == null ?
                String.join(" ", "list objects from", getSourceName(), "bucket:", bucket) :
                String.join(" ", "list objects from", getSourceName(), "bucket:", bucket, "and", processor.getProcessName());
        rootLogger.info("{} running...", info);
        rootLogger.info("order\tprefix\tquantity");
        showdownHook();
        FileSaveMapper.append = false; // 默认让持久化非追加写入（即清除之前存在的文件）
        ILister<E> startLister = null;
        if (prefixes == null || prefixes.size() == 0) {
            startLister = generateLister("");
            if (threads > 1) {
                prefixes = moreValidPrefixes(startLister, false);
                if (prefixes == null) threads = 1;
            }
            if (threads <= 1) {
                listing(startLister);
                rootLogger.info("{} finished.", info);
                endAction();
                return;
            }
        } else {
            if (prefixLeft && prefixes.get(0).compareTo("") > 0) {
                insertIntoPrefixesMap("", new HashMap<String, String>(){{ put("end", prefixes.get(0)); }});
                startLister = generateLister("");
            }
            prefixes = prefixes.parallelStream().filter(this::checkPrefix).peek(this::recordListerByPrefix)
                    .collect(Collectors.toList());
        }
        executorPool = Executors.newFixedThreadPool(threads);
        try {
            if (startLister != null) processNodeLister(startLister);
            List<ILister<E>> listerList = filteredListerByPrefixes(prefixes.parallelStream());
            while (listerList != null && listerList.size() > 0 && listerList.size() < threads) {
                prefixesMap.clear();
                listerList = computeToNextLevel(listerList);
            }
            if (listerList != null && listerList.size() > 0) {
                listerList.parallelStream().forEach(lister -> executorPool.execute(() -> listing(lister)));
            }
            executorPool.shutdown();
            waitAndTailListing(listerList);
            rootLogger.info("{} finished.", info);
            endAction();
        } catch (Throwable e) {
//            executorPool.shutdownNow(); // 执行中的 sleep(), wait() 操作会抛出 InterruptedException
            pauseDateTime = LocalDateTime.MIN; // 使用该语句使线程池中的任务停滞效果可能比 shutdownNow() 要好
            rootLogger.error("export failed", e);
            endAction();
            System.exit(-1);
        }
    }

    private final Object object = new Object();
    private LocalDateTime pauseDateTime = LocalDateTime.MAX;

    public void export(LocalDateTime startTime, long pauseDelay, long duration) throws Exception {
        if (startTime != null) {
            Clock clock = Clock.systemDefaultZone();
            LocalDateTime now = LocalDateTime.now(clock);
            if (startTime.minusWeeks(1).isAfter(now)) {
                throw new Exception("startTime is not allowed to exceed next week");
            }
            while (now.isBefore(startTime)) {
                System.out.printf("\r%s", LocalDateTime.now(clock).toString().substring(0, 19));
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
                now = LocalDateTime.now(clock);
            }
        }
        if (duration <= 0 || pauseDelay < 0) {
            export();
        } else if (duration > 84600 || duration < 1800) {
            throw new Exception("duration can not be bigger than 23.5 hours or smaller than 0.5 hours.");
        } else {
            pauseDateTime = LocalDateTime.now().plusSeconds(pauseDelay);
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    synchronized (object) {
                        object.notifyAll();
                    }
                    pauseDateTime = LocalDateTime.now().plusSeconds(86400 - duration);
//                    pauseDateTime = LocalDateTime.now().plusSeconds(20 - duration);
                }
            }, (pauseDelay + duration) * 1000, 86400000);
//            }, (pauseDelay + duration) * 1000, 20000);
            export();
            timer.cancel();
        }
    }
}
