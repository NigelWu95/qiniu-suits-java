package com.qiniu.datasource;

import com.google.gson.JsonObject;
import com.qiniu.common.QiniuException;
import com.qiniu.common.SuitsException;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.qiniu.entry.CommonParams.lineFormats;

public abstract class CloudStorageContainer<E, W, T> implements IDataSource<ILister<E>, IResultOutput<W>, T> {

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
    protected ExecutorService executorPool; // 线程池
    protected ILineProcess<T> processor; // 定义的资源处理器
    protected List<String> originPrefixList = new ArrayList<>();
    public static String firstPoint;
    private String lastPoint;
    private ConcurrentMap<String, Map<String, String>> prefixAndEndedMap = new ConcurrentHashMap<>();
    ConcurrentMap<String, IResultOutput<W>> saverMap = new ConcurrentHashMap<>();
    ConcurrentMap<String, ILineProcess<T>> processorMap = new ConcurrentHashMap<>();

    public CloudStorageContainer(String bucket, List<String> antiPrefixes, Map<String, Map<String, String>> prefixesMap,
                                 boolean prefixLeft, boolean prefixRight, Map<String, String> indexMap, int unitLen,
                                 int threads) {
        this.bucket = bucket;
        // 先设置 antiPrefixes 后再设置 prefixes，因为可能需要从 prefixes 中去除 antiPrefixes 含有的元素
        this.antiPrefixes = antiPrefixes;
        if (antiPrefixes != null && antiPrefixes.size() > 0) hasAntiPrefixes = true;
        this.prefixLeft = prefixLeft;
        this.prefixRight = prefixRight;
        setPrefixesAndMap(prefixesMap);
        this.unitLen = unitLen;
        this.threads = threads;
        // default save parameters
        this.saveTotal = true; // 默认全记录保存
        this.savePath = "result";
        this.saveFormat = "tab";
        this.saveSeparator = "\t";
        setIndexMapWithDefault(indexMap);
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
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes < 1 ? 5 : retryTimes;
    }

    private void setIndexMapWithDefault(Map<String, String> indexMap) {
        if (indexMap == null || indexMap.size() == 0) {
            if (this.indexMap == null) this.indexMap = new HashMap<>();
            for (String fileInfoField : ConvertingUtils.defaultFileFields) {
                this.indexMap.put(fileInfoField, fileInfoField);
            }
        } else {
            this.indexMap = indexMap;
        }
    }

    public void setProcessor(ILineProcess<T> processor) {
        this.processor = processor;
    }

    private void setPrefixesAndMap(Map<String, Map<String, String>> prefixesMap) {
        if (prefixesMap == null || prefixesMap.size() <= 0) {
            this.prefixesMap = new HashMap<>();
            prefixLeft = true;
            prefixRight = true;
            if (hasAntiPrefixes) {
                prefixes = originPrefixList.stream().filter(this::checkPrefix).sorted().collect(Collectors.toList());
            }
        } else {
            this.prefixesMap = prefixesMap;
            prefixes = prefixesMap.keySet().parallelStream().filter(this::checkPrefix).sorted().collect(Collectors.toList());
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

    private volatile JsonObject prefixesJson = new JsonObject();

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
                "listing remained files.\n", fileName, FileSaveMapper.ext);
    }

    JsonObject recordListerByPrefix(String prefix) {
        JsonObject json = prefixesMap.get(prefix) == null ? null : JsonUtils.toJsonObject(prefixesMap.get(prefix));
        recordPrefixConfig(prefix, json);
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
        ITypeConvert<E, String> stringConverter = saveTotal ? getNewStringConverter() : null;
        List<T> convertedList;
        List<String> writeList;
        List<E> objects = lister.currents();
        boolean hasNext = lister.hasNext();
        int retry;
        Map<String, String> map = null;
        // 初始化的 lister 包含首次列举的结果列表，需要先取出，后续向前列举时会更新其结果列表
        while (objects.size() > 0 || hasNext) {
            if (stringConverter != null) {
                writeList = stringConverter.convertToVList(objects);
                if (writeList.size() > 0) saver.writeSuccess(String.join("\n", writeList), false);
                if (stringConverter.errorSize() > 0) saver.writeKeyFile("error", stringConverter.errorLines(), false);
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
                e.response.close();
            }
            if (hasNext) {
                JsonObject json = JsonUtils.getOrNew(prefixesJson, lister.getPrefix());
                json.addProperty("marker", lister.getMarker());
                json.addProperty("end", lister.getEndPrefix());
                recordPrefixConfig(lister.getPrefix(), json);
                if (objects.size() <= 0) map = prefixAndEndedMap.get(lister.getPrefix());
            } else {
                map = prefixAndEndedMap.get(lister.getPrefix());
            }
            if (map != null) map.put("start", lister.currentEndKey());
            retry = retryTimes;
            while (true) {
                try {
                    lister.listForward(); // 要求 listForward 实现中先做 hashNext 判断，if (!hasNext) clear();
                    objects = lister.currents();
                    break;
                } catch (SuitsException e) {
                    retry = HttpRespUtils.listExceptionWithRetry(e, retry);
                    System.out.println("list objects by prefix:" + lister.getPrefix() + " retrying... " + e.getMessage());
                }
            }
            hasNext = lister.hasNext();
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
        String orderStr = String.valueOf(order);
        IResultOutput<W> saver = null;
        ILineProcess<T> lineProcessor = null;
        try {
            // 多线程情况下不要直接使用传入的 processor，因为对其关闭会造成 clone 的对象无法进行结果持久化的写入
            if (processor != null) {
                lineProcessor = processor.clone();
                processorMap.put(orderStr, lineProcessor);
            }
            saver = getNewResultSaver(orderStr);
            saverMap.put(orderStr, saver);
            export(lister, saver, lineProcessor);
            removePrefixConfig(lister.getPrefix());
            saverMap.remove(orderStr);
            System.out.println("order " + orderStr + ": " + lister.getPrefix() + "\tsuccessfully done");
        } catch (Throwable e) {
            e.printStackTrace();
            System.out.println("order " + orderStr + ": " + lister.getPrefix() + "\t" + prefixesJson.get(lister.getPrefix()));
            Map<String, String> map = prefixAndEndedMap.get(lister.getPrefix());
            if (map != null) map.put("start", lister.currentEndKey());
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
                System.out.println("generate lister by prefix:" + prefix + " retrying... " + e.getMessage());
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
            Map<String, String> map = prefixesMap.get(lastLister.getPrefix());
            if (map == null) map = new HashMap<>();
            map.remove("marker");
            prefixAndEndedMap.put(lastLister.getPrefix(), map);
        }
        Iterator<ILister<E>> it = nextLevelList.iterator();
        while (it.hasNext()) {
            ILister<E> nLister = it.next();
            if(!nLister.hasNext() || (nLister.getEndPrefix() != null && !"".equals(nLister.getEndPrefix()))) {
                executorPool.execute(() -> listing(nLister, UniOrderUtils.getOrder()));
                it.remove();
            }
        }
        return nextLevelList;
    }

    private void processNodeLister(ILister<E> lister) {
        if (lister.currents().size() > 0 || lister.hasNext()) {
            executorPool.execute(() -> listing(lister, UniOrderUtils.getOrder()));
        } else {
            removePrefixConfig(lister.getPrefix());
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

    private List<String> checkListerInPool(List<ILister<E>> listerList) throws Exception {
        List<String> extremePrefixes = null;
        int cValue = threads < 10 ? 3 : threads / 2;
        int tiny = threads >= 300 ? 30 : threads >= 200 ? 20 : threads >= 100 ? 10 : threads >= 50 ? threads / 10 :
                threads >= 10 ? 3 : 1;
        int count = 0;
        boolean startCheck = false;
        ILister<E> iLister;
        Iterator<ILister<E>> iterator;
        while (!executorPool.isTerminated()) {
            try {
                if (count >= 300) {
                    iterator = listerList.iterator();
                    while (iterator.hasNext()) {
                        iLister = iterator.next();
                        if(!iLister.hasNext()) iterator.remove();
                    }
                    if (startCheck && listerList.size() <= tiny) {
                        System.out.printf("unfinished: %s, cValue: %s\nto re-split prefixes...", listerList.size(), cValue);
                        for (ILister<E> lister : listerList) {
                            String prefix = lister.getPrefix();
                            String nextMarker = lister.truncate();
                            if (nextMarker == null) continue;
                            if (extremePrefixes == null) extremePrefixes = new ArrayList<>();
                            extremePrefixes.add(prefix);
                            insertIntoPrefixesMap(prefix, new HashMap<String, String>(){{ put("marker", nextMarker); }});
                        }
                    }
                    if (startCheck || listerList.size() > cValue) {
                        count = 0;
                    } else {
                        startCheck = true;
                        count = listerList.size() <= tiny ? -1800 : -3200;
                    }
                }
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
        String lastPrefix;
        Map<String, String> prefixMap;
        Map<String, String> lastPrefixMap;
        for (String prefix : phraseLastPrefixes) {
            prefixMap = prefixAndEndedMap.get(prefix);
            if (prefixMap.size() == 0) {
                prefixAndEndedMap.remove(prefix);
                continue;
            }
            removePrefixConfig(prefix);
            lastPrefix = prefix.substring(0, prefix.length() - 1);
            lastPrefixMap = prefixAndEndedMap.get(lastPrefix);
            if (lastPrefixMap != null) {
                prefixAndEndedMap.put(lastPrefix, prefixMap);
                prefixAndEndedMap.remove(prefix);
            } else {
                if (!"".equals(lastPrefix) || prefixRight) {
                    prefixAndEndedMap.put(lastPrefix, prefixMap);
                    prefixAndEndedMap.remove(prefix);
                }
            }
        }
        prefixesMap.putAll(prefixAndEndedMap);
        phraseLastPrefixes = new ArrayList<>(prefixAndEndedMap.keySet());
        for (String phraseLastPrefix : phraseLastPrefixes) recordListerByPrefix(phraseLastPrefix);
        return phraseLastPrefixes;
    }

    private void waitAndTailListing(List<ILister<E>> listerList) throws Exception {
        List<String> extremePrefixes = checkListerInPool(listerList);
        while (extremePrefixes != null && extremePrefixes.size() > 0) {
            executorPool = Executors.newFixedThreadPool(threads);
            listerList = filteredListerByPrefixes(extremePrefixes.parallelStream());
            while (listerList != null && listerList.size() > 0 && listerList.size() <= threads) {
                listerList = computeToNextLevel(listerList);
            }
            if (listerList != null && listerList.size() > 0) {
                listerList.parallelStream().forEach(lister -> executorPool.execute(() -> listing(lister, UniOrderUtils.getOrder())));
            }
            executorPool.shutdown();
            extremePrefixes = checkListerInPool(listerList);
        }
        List<String> phraseLastPrefixes = lastEndedPrefixes();
        if (phraseLastPrefixes.size() > 0) {
            executorPool = Executors.newFixedThreadPool(phraseLastPrefixes.size());
            listerList = filteredListerByPrefixes(phraseLastPrefixes.parallelStream());
            listerList.parallelStream().forEach(lister -> executorPool.execute(() -> listing(lister, UniOrderUtils.getOrder())));
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

    /**
     * 根据当前参数值创建多线程执行数据源导出工作
     */
    public void export() throws Exception {
        String process = null;
        if (processor != null) {
            if (processor.getNextProcessor() != null) {
                process = processor.getNextProcessor().getProcessName() + " with " + processor.getProcessName();
            } else {
                process = processor.getProcessName();
            }
        }
        String info = "list objects from bucket: " + bucket + (process == null ? "" : " and " + process);
        System.out.println(info + " running...");
        ILister<E> startLister = null;
        if (prefixes == null || prefixes.size() == 0) {
            startLister = generateLister("");
            if (threads > 1) {
                prefixes = moreValidPrefixes(startLister, false);
                if (prefixes == null) threads = 1;
            }
            if (threads <= 1) {
                int order = UniOrderUtils.getOrder();
                listing(startLister, order);
                return;
            }
        } else {
            for (String prefix : prefixes) recordListerByPrefix(prefix);
            if (prefixLeft) {
                insertIntoPrefixesMap("", new HashMap<String, String>(){{ put("end", prefixes.get(0)); }});
                startLister = generateLister("");
            }
        }
        executorPool = Executors.newFixedThreadPool(threads);
        try {
            if (startLister != null) processNodeLister(startLister);
            List<ILister<E>> listerList = filteredListerByPrefixes(prefixes.parallelStream());
            while (listerList != null && listerList.size() > 0 && listerList.size() < threads) {
                listerList = computeToNextLevel(listerList);
            }
            if (listerList != null && listerList.size() > 0) {
                listerList.parallelStream().forEach(lister -> executorPool.execute(() -> listing(lister, UniOrderUtils.getOrder())));
            }
            executorPool.shutdown();
            waitAndTailListing(listerList);
            System.out.println(info + " finished.");
        } catch (Throwable e) {
            executorPool.shutdownNow();
            e.printStackTrace();
            ILineProcess<T> processor;
            for (Map.Entry<String, IResultOutput<W>> saverEntry : saverMap.entrySet()) {
                saverEntry.getValue().closeWriters();
                processor = processorMap.get(saverEntry.getKey());
                if (processor != null) processor.closeResource();
            }
        } finally {
            writeContinuedPrefixConfig(savePath, "prefixes");
        }
    }
}
