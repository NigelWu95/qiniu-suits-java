package com.qiniu.datasource;

import com.google.gson.JsonObject;
import com.qiniu.common.QiniuException;
import com.qiniu.common.SuitsException;
import com.qiniu.interfaces.IDataSource;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.interfaces.IStorageLister;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.interfaces.IResultOutput;
import com.qiniu.util.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class CloudStorageContainer<E, T> extends DatasourceActor implements IDataSource<IStorageLister<E>, IResultOutput, T> {

    protected String bucket;
    protected List<String> antiPrefixes;
    protected boolean hasAntiPrefixes = false;
    protected ConcurrentMap<String, Map<String, String>> prefixesMap;
    protected List<String> prefixes;
    protected boolean prefixLeft;
    protected boolean prefixRight;
    protected ILineProcess<T> processor; // 定义的资源处理器
    protected List<String> originPrefixList;
    static String firstPoint;
    private String lastPoint;
    private ConcurrentMap<String, Map<String, String>> prefixAndEndedMap = new ConcurrentHashMap<>(100);

    public CloudStorageContainer(String bucket, Map<String, Map<String, String>> prefixesMap, List<String> antiPrefixes,
                                 boolean prefixLeft, boolean prefixRight, Map<String, String> indexMap, List<String> fields,
                                 int unitLen, int threads) throws IOException {
        super(unitLen, threads);
        this.bucket = bucket;
        this.prefixLeft = prefixLeft;
        this.prefixRight = prefixRight;
        // 先设置 antiPrefixes 后再设置 prefixes，因为可能需要从 prefixes 中去除 antiPrefixes 含有的元素
        setAntiPrefixes(antiPrefixes);
        setPrefixesAndMap(prefixesMap);
        setIndexMapWithDefault(indexMap);
        if (fields != null && fields.size() > 0) this.fields = fields;
        else this.fields = ConvertingUtils.getOrderedFields(this.indexMap, null);
        // default save parameters，默认全记录保存
        setSaveOptions(true, "result", "tab", "\t", null);
        // 由于目前指定包含 "|" 字符的前缀列举会导致超时，因此先将该字符及其 ASCII 顺序之前的 "{" 和之后的（"|}~"）统一去掉，从而优化列举的超
        // 时问题，简化前缀参数的设置，也避免为了兼容该字符去修改代码算法
        originPrefixList = Arrays.asList(
                (" !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz").split("")
        );
        firstPoint = originPrefixList.get(0);
        lastPoint = originPrefixList.get(originPrefixList.size() - 1);
    }

    private void setAntiPrefixes(List<String> antiPrefixes) {
        if (antiPrefixes != null && antiPrefixes.size() > 0) {
            hasAntiPrefixes = true;
            this.antiPrefixes = antiPrefixes.stream().sorted().collect(Collectors.toList());
            int size = this.antiPrefixes.size();
            Iterator<String> iterator = this.antiPrefixes.iterator();
            String temp = iterator.next();
            while (iterator.hasNext() && size > 0) {
                size--;
                String prefix = iterator.next();
                if (prefix.startsWith(temp)) iterator.remove();
                else temp = prefix;
            }
        }
    }

    private void setPrefixesAndMap(Map<String, Map<String, String>> prefixesMap) throws IOException {
        if (prefixesMap == null || prefixesMap.size() <= 0) {
            this.prefixesMap = new ConcurrentHashMap<>(threads);
            prefixLeft = true;
            prefixRight = true;
            if (hasAntiPrefixes && !"upyun".equals(getSourceName())) prefixes = originPrefixList;
        } else {
            if (prefixesMap.containsKey(null) || prefixesMap.containsValue(null)) throw new IOException("prefixes map can not contain null.");
            this.prefixesMap = new ConcurrentHashMap<>(threads);
            this.prefixesMap.putAll(prefixesMap);
            prefixes = prefixesMap.keySet().stream().sorted().distinct().collect(Collectors.toList());
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
            } else {
                end = value == null ? null : value.get("end");
            }
            while (iterator.hasNext() && size > 0) {
                size--;
                String prefix = iterator.next();
                if (prefix.startsWith(temp)) {
                    if (end == null || "".equals(end)) {
                        iterator.remove();
                        this.prefixesMap.remove(prefix);
                    } else if (end.compareTo(prefix) >= 0) {
                        throw new IOException(temp + "'s end can not be larger than " + prefix + " in " + prefixesMap);
                    }
                } else {
                    temp = prefix;
                    value = prefixesMap.get(temp);
                    end = value == null ? null : value.get("end");
                }
            }
        }
        if (hasAntiPrefixes && prefixes != null && prefixes.size() > 0) {
            antiPrefixes.sort(Comparator.naturalOrder());
            if (firstPoint.compareTo(antiPrefixes.get(0)) > 0) {
                throw new IOException("min anti-prefix can not be smaller than prefix: " + firstPoint);
            }
            String firstPrefix = prefixes.get(0);
            if (prefixLeft && firstPrefix.compareTo(antiPrefixes.get(0)) >= 0) {
                throw new IOException("with prefix-left, min anti-prefix can not be smaller than min prefix: " + firstPrefix);
            }
            String lastAntiPrefix = antiPrefixes.get(antiPrefixes.size() - 1);
            if (lastPoint.compareTo(lastAntiPrefix) < 0) {
                throw new IOException("max anti-prefix can not be larger than prefix： " + lastPoint);
            }
            String lastPrefix = prefixes.get(prefixes.size() - 1);
            if (prefixRight && lastPrefix.compareTo(lastAntiPrefix) <= 0) {
                throw new IOException("with prefix-right, max anti-prefix can not be same as or larger than max prefix： " + lastPoint);
            }
        }
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

    /**
     * 检验 prefix 是否有效，在 antiPrefixes 前缀列表中或者为空均无效
     * @param prefix 待检验的 prefix
     * @return 检验结果，true 表示 prefix 有效不需要剔除
     */
    boolean checkPrefix(String prefix) {
//        if (prefix == null) return false;
//        if (hasAntiPrefixes) {
            for (String antiPrefix : antiPrefixes) {
                if (prefix.startsWith(antiPrefix)) return false;
            }
            return true;
//        } else {
//            return true;
//        }
    }

    protected abstract ITypeConvert<E, T> getNewConverter();

    protected abstract ITypeConvert<E, String> getNewStringConverter() throws IOException;

    /**
     * 执行列举操作，直到当前的 lister 列举结束，并使用 processor 对象执行处理过程
     * @param lister 已经初始化的 lister 对象
     * @param saver 用于列举结果持久化的文件对象
     * @param processor 用于资源处理的处理器对象
     * @throws IOException 列举出现错误或者持久化错误抛出的异常
     */
    public void export(IStorageLister<E> lister, IResultOutput saver, ILineProcess<T> processor) throws Exception {
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
        JsonObject json = map != null ? JsonUtils.toJsonObject(map) : (hasNext ? new JsonObject() : null);
        // 初始化的 lister 包含首次列举的结果列表，需要先取出，后续向前列举时会更新其结果列表
        while (objects.size() > 0 || hasNext) {
            if (stopped) break;
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
                json.addProperty("marker", lister.getMarker());
                recordLister(lister.getPrefix(), json.toString());
            }
            if (map != null) map.put("start", lister.currentEndKey());
            statistics.addAndGet(objects.size());
            if (stopped) break;
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

    protected abstract IResultOutput getNewResultSaver(String order) throws IOException;

    /**
     * 将 lister 对象放入线程池进行执行列举，如果 processor 不为空则同时执行 process 过程
     * @param lister 列举对象
     */
    void listing(IStorageLister<E> lister) {
        // 持久化结果标识信息
        int order = UniOrderUtils.getOrder();
        String orderStr = String.valueOf(order);
        IResultOutput saver = null;
        ILineProcess<T> lineProcessor = null;
        try {
            saver = getNewResultSaver(orderStr);
            saverMap.put(orderStr, saver);
            // 多线程情况下不要直接使用传入的 processor，因为对其关闭会造成 clone 的对象无法进行结果持久化的写入
            if (processor != null) {
                lineProcessor = processor.clone();
                lineProcessor.changeSaveOrder(orderStr);
                processorMap.put(orderStr, lineProcessor);
            }
            export(lister, saver, lineProcessor);
            procedureLogger.info("{}-|-", lister.getPrefix());
            progressMap.remove(lister.getPrefix()); // 只有 export 成功情况下才移除 record
        } catch (QiniuException e) {
            try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
            errorLogger.error("{}: {}, {}", lister.getPrefix(), progressMap.get(lister.getPrefix()), e.error(), e);
            if (e.response != null) e.response.close();
        } catch (Throwable e) {
            try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
            errorLogger.error("{}: {}", lister.getPrefix(), progressMap.get(lister.getPrefix()), e);
        } finally {
            try { FileUtils.createIfNotExists(infoLogFile); } catch (IOException ignored) {}
            infoLogger.info("{}\t{}\t{}", orderStr, lister.getPrefix(), lister.count());
            if (saver != null) {
                saver.closeWriters();
                saver = null; // let gc work
            }
            saverMap.remove(orderStr);
            processorMap.remove(orderStr);
            if (lineProcessor != null) {
                lineProcessor.closeResource();
                lineProcessor = null;
            }
            UniOrderUtils.returnOrder(order); // 最好执行完 close 再归还 order，避免上个文件描述符没有被使用，order 又被使用
            lister.close();
        }
    }

    protected abstract IStorageLister<E> getLister(String prefix, String marker, String start, String end, int unitLen) throws SuitsException;

    IStorageLister<E> generateLister(String prefix) throws SuitsException {
        return generateLister(prefix, 0);
    }

    private IStorageLister<E> generateLister(String prefix, int limit) throws SuitsException {
        limit = limit > 0 ? limit : unitLen;
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
                return getLister(prefix, marker, start, end, limit);
            } catch (SuitsException e) {
                retry = HttpRespUtils.listExceptionWithRetry(e, retry);
                try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
                errorLogger.error("generate lister by prefix:{} retrying...", prefix, e);
            }
        }
    }

    private String nextPoint(IStorageLister<E> lister, boolean doFutureCheck) {
        boolean next;
        try {
            next = doFutureCheck ? lister.hasFutureNext() : lister.hasNext();
        } catch (SuitsException e) {
            errorLogger.warn("check lister hasFutureNext of \"{}\" has error: ", lister.getPrefix(), e);
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
                        lister.setLimit(2);
                    } else {
                        prefixesMap.put(startPrefix + point, new HashMap<String, String>(){{ put("marker", lister.getMarker()); }});
                        lister.setEndPrefix(endKey);
                    }
                } else {
                    point = firstPoint;
                    // 无 next 时直接将 lister 的结束位置设置到第一个预定义前
                    lister.setEndPrefix(startPrefix + firstPoint);
                    lister.setLimit(2);
                }
            } else {
                return nextPoint(lister, true);
            }
        }
        return point;
    }

    private List<IStorageLister<E>> filteredListerByPrefixes(Stream<String> prefixesStream) {
        List<IStorageLister<E>> prefixesLister = prefixesStream.map(prefix -> {
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
                progressMap.remove(generated.getPrefix());
                generated.close();
                return false;
            }
        }).collect(Collectors.toList());
        if (prefixesLister.size() > 0) {
            IStorageLister<E> lastLister = prefixesLister.stream().max(Comparator.comparing(IStorageLister::getPrefix)).get();
            Map<String, String> map = prefixesMap.get(lastLister.getPrefix());
            if (map == null) {
                prefixAndEndedMap.put(lastLister.getPrefix(), new HashMap<>());
            } else if (!map.containsKey("remove")) {
                prefixAndEndedMap.put(lastLister.getPrefix(), map);
            }
        }
        Iterator<IStorageLister<E>> it = prefixesLister.iterator();
        while (it.hasNext()) {
            IStorageLister<E> nLister = it.next();
            if(!nLister.hasNext() || (nLister.getEndPrefix() != null && !"".equals(nLister.getEndPrefix()))) {
                executorPool.execute(() -> listing(nLister));
                it.remove();
            }
        }
        return prefixesLister;
    }

    private void processNodeLister(IStorageLister<E> lister) {
        if (lister.currents().size() > 0 || lister.hasNext()) {
            executorPool.execute(() -> listing(lister));
        } else {
            progressMap.remove(lister.getPrefix());
            lister.close();
        }
    }

    void recordListerByPrefix(String prefix) {
        Map<String, String> map = prefixesMap.get(prefix);
        String record = map == null ? "{}" : JsonUtils.toJsonObject(map).toString();
        recordLister(prefix, record);
    }

    private List<IStorageLister<E>> computeToNextLevel(List<IStorageLister<E>> listerList) {
        if (hasAntiPrefixes) {
            return listerList.parallelStream().map(lister -> {
                List<String> nextPrefixes = null;
                String finalPoint = nextPoint(lister, true);
                if (finalPoint != null) {
                    nextPrefixes = originPrefixList.stream().filter(prefix -> prefix.compareTo(finalPoint) >= 0)
                            .map(prefix -> lister.getPrefix() + prefix).filter(this::checkPrefix)
                            .peek(this::recordListerByPrefix).collect(Collectors.toList());
                }
                processNodeLister(lister);
                if (nextPrefixes != null) return filteredListerByPrefixes(nextPrefixes.stream());
                else return null;
            }).filter(Objects::nonNull).reduce((list1, list2) -> { list1.addAll(list2); return list1; }).orElse(null);
        } else {
            return listerList.parallelStream().map(lister -> {
                List<String> nextPrefixes = null;
                String finalPoint = nextPoint(lister, true);
                if (finalPoint != null) {
                    nextPrefixes = originPrefixList.stream().filter(prefix -> prefix.compareTo(finalPoint) >= 0)
                            .map(prefix -> lister.getPrefix() + prefix)
                            .peek(this::recordListerByPrefix).collect(Collectors.toList());
                }
                processNodeLister(lister);
                if (nextPrefixes != null) return filteredListerByPrefixes(nextPrefixes.stream());
                else return null;
            }).filter(Objects::nonNull).reduce((list1, list2) -> { list1.addAll(list2); return list1; }).orElse(null);
        }
    }

    private List<String> checkListerInPool(List<IStorageLister<E>> listerList, int cValue, int initTiny) {
        List<String> extremePrefixes = null;
        int count = 0;
        IStorageLister<E> iLister;
        Iterator<IStorageLister<E>> iterator;
        String prefix;
        String nextMarker;
        String start;
        Map<String, String> endMap;
        Map<String, String> prefixMap;
        int tiny = initTiny;
        int accUnit = initTiny / 2;
        while (!executorPool.isTerminated()) {
            if (count >= 1200) {
                if (listerList == null) {
                    count = 0;
                    continue;
                }
                iterator = listerList.iterator();
                while (iterator.hasNext()) {
                    iLister = iterator.next();
                    if(!iLister.hasNext()) iterator.remove();
                }
                if (listerList.size() > 0 && listerList.size() <= tiny) {
                    tiny = initTiny;
                    rootLogger.info("unfinished: {}, cValue: {}, to re-split prefixes...", listerList.size(), cValue);
                    for (IStorageLister<E> lister : listerList) {
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
                        rootLogger.info("prefix: {}, nextMarker: {}, endMap: {}", prefix, nextMarker, endMap);
                        // 如果 truncate 时的 nextMarker 已经为空说明已经列举完成了
                        if (nextMarker == null || nextMarker.isEmpty()) continue;
                        if (extremePrefixes == null) extremePrefixes = new ArrayList<>();
                        extremePrefixes.add(prefix);
                        prefixMap.put("marker", nextMarker);
                        prefixesMap.put(prefix, prefixMap);
                    }
                } else if (listerList.size() <= cValue) {
                    tiny += accUnit;
                    count = 900;
                } else {
                    count = 0;
                }
                refreshRecordAndStatistics();
            }
            sleep(1000);
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
            // 由于 marker 优先原则，为了 start 生效则将可能的 marker 删除
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
        phraseLastPrefixes = prefixAndEndedMap.keySet().stream().sorted()
                .peek(this::recordListerByPrefix).collect(Collectors.toList());
        return phraseLastPrefixes;
    }

    private void prefixesListing() {
        List<IStorageLister<E>> listerList = filteredListerByPrefixes(prefixes.parallelStream());
        while (listerList != null && listerList.size() > 0 && listerList.size() < threads) {
            prefixesMap.clear();
            listerList = computeToNextLevel(listerList);
        }
        if (listerList != null && listerList.size() > 0) {
            listerList.parallelStream().forEach(lister -> executorPool.execute(() -> listing(lister)));
        }
        executorPool.shutdown();
        if (threads > 1) {
            int cValue = threads >= 10 ? threads / 2 : 3;
            int tiny = threads >= 30 ? threads / 10 : threads >= 10 ? 3 : 1;
            List<String> extremePrefixes = checkListerInPool(listerList, cValue, tiny);
            while (extremePrefixes != null && extremePrefixes.size() > 0) {
                extremePrefixes.parallelStream().forEach(this::recordListerByPrefix);
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
        } else {
            while (!executorPool.isTerminated()) {
                sleep(2000);
                if (countInterval-- <= 0) {
                    countInterval = 300;
                    refreshRecordAndStatistics();
                }
            }
        }
        List<String> phraseLastPrefixes = lastEndedPrefixes();
        if (phraseLastPrefixes.size() > 0) {
            executorPool = Executors.newFixedThreadPool(phraseLastPrefixes.size());
            listerList = filteredListerByPrefixes(phraseLastPrefixes.parallelStream());
            listerList.parallelStream().forEach(lister -> executorPool.execute(() -> listing(lister)));
            executorPool.shutdown();
        }
        while (!executorPool.isTerminated()) {
            sleep(2000);
            if (countInterval-- <= 0) {
                countInterval = 300;
                refreshRecordAndStatistics();
            }
        }
    }

    /**
     * 根据当前参数值创建多线程执行数据源导出工作
     */
    @Override
    public void export() throws Exception {
        String info = processor == null ? String.join(" ", "list objects from", getSourceName(), "bucket:", bucket) :
                String.join(" ", "list objects from", getSourceName(), "bucket:", bucket, "and", processor.getProcessName());
        rootLogger.info("{} running...", info);
        rootLogger.info("order\tprefix\tquantity");
        showdownHook();
        IStorageLister<E> startLister = null;
        // 在初始化时即做检查，hasAntiPrefixes 的情况下 prefixes 不可能为空，所以在 prefixes 为空时，hasAntiPrefixes 一定为 false
        if (prefixes == null || prefixes.size() == 0) {
            recordListerByPrefix("");
            startLister = generateLister("", 2);
            startLister.setLimit(unitLen);
            if (threads > 1) {
                String finalPoint = nextPoint(startLister, false);
                if (finalPoint != null) {
                    IStorageLister<E> lister = startLister;
                    prefixes = originPrefixList.parallelStream().filter(prefix -> prefix.compareTo(finalPoint) >= 0)
                            .map(prefix -> lister.getPrefix() + prefix).collect(Collectors.toList());
                }
            }
        } else {
            if (prefixLeft || "".equals(prefixes.get(0))) {
                recordListerByPrefix("");
                if ("".equals(prefixes.get(0))) prefixes.remove(0);
                prefixesMap.put("", new HashMap<String, String>(){{ put("end", prefixes.get(0)); }});
                startLister = generateLister("", 2);
                startLister.setLimit(unitLen);
                prefixesMap.remove("");
            }
        }
        try {
            if (prefixes == null || prefixes.size() == 0) {
                if (hasAntiPrefixes) rootLogger.info("there are no prefixes to check anti-prefixes.");
                if (startLister.currents().size() > 0 || startLister.hasNext()) {
                    listing(startLister);
                } else {
                    progressMap.remove(startLister.getPrefix());
                    startLister.close();
                }
            } else {
                if (hasAntiPrefixes) {
                    prefixes = prefixes.parallelStream().filter(this::checkPrefix)
                            .peek(this::recordListerByPrefix).collect(Collectors.toList());
                } else {
                    prefixes.parallelStream().forEach(this::recordListerByPrefix);
                }
                executorPool = Executors.newFixedThreadPool(threads);
                if (startLister != null) processNodeLister(startLister);
                prefixesListing();
            }
            rootLogger.info("{} finished, results in {}.", info, savePath);
            endAction();
        } catch (Throwable e) {
//            executorPool.shutdownNow(); // 执行中的 sleep(), wait() 操作会抛出 InterruptedException
            stopped = true; // 使用该语句使线程池中的任务退出循环可能比直接 shutdownNow() 要好
            rootLogger.error("export failed", e);
            endAction();
            System.exit(-1);
        }
    }
}
