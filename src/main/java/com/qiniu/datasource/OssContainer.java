package com.qiniu.datasource;

import com.qiniu.common.QiniuException;
import com.qiniu.common.SuitsException;
import com.qiniu.entry.CommonParams;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.IResultOutput;
import com.qiniu.util.HttpRespUtils;
import com.qiniu.util.LineUtils;
import com.qiniu.util.SystemUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public abstract class OssContainer<E, W, T> implements IDataSource<ILister<E>, IResultOutput<W>, T> {

    protected String bucket;
    private List<String> antiPrefixes;
    private Map<String, String[]> prefixesMap;
    private List<String> prefixes;
    private boolean prefixLeft;
    private boolean prefixRight;
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
    private List<String> originPrefixList = new ArrayList<>();
    private ILineProcess<T> processor; // 定义的资源处理器

    public OssContainer(String bucket, List<String> antiPrefixes, Map<String, String[]> prefixesMap, boolean prefixLeft,
                        boolean prefixRight, Map<String, String> indexMap, int unitLen, int threads) {
        this.bucket = bucket;
        // 先设置 antiPrefixes 后再设置 prefixes，因为可能需要从 prefixes 中去除 antiPrefixes 含有的元素
        this.antiPrefixes = antiPrefixes;
        this.prefixesMap = prefixesMap == null ? new HashMap<>() : prefixesMap;
        setPrefixes();
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
        prefixesMap = commonParams.getPrefixesMap();
        if (prefixesMap == null) prefixesMap = new HashMap<>();
        setPrefixes();
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

    private void setPrefixes() {
        prefixes = new ArrayList<>();
        for (String prefix : prefixesMap.keySet()) {
            if (checkPrefix(prefix)) prefixes.add(prefix);
        }
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

    protected abstract ITypeConvert<E, T> getNewConverter();

    protected abstract ITypeConvert<E, String> getNewStringConverter() throws IOException;

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
                    if (HttpRespUtils.checkStatusCode(e.getStatusCode()) < 0) throw e;
                    else if (retry <= 0 && e.getStatusCode() >= 500) throw e;
                    else retry--;
                }
            }
        }
    }

    protected abstract IResultOutput<W> getNewResultSaver(String order) throws IOException;

    /**
     * 将 lister 对象放入线程池进行执行列举，如果 processor 不为空则同时执行 process 过程
     * @param lister 列举对象
     * @param order 当前列举对象集的起始序号
     * @throws Exception 操作失败抛出的异常
     */
    public void execInThread(ILister<E> lister, int order) throws Exception {
        // 如果是第一个线程直接使用初始的 processor 对象，否则使用 clone 的 processor 对象，多线程情况下不要直接使用传入的 processor，
        // 因为对其关闭会造成 clone 的对象无法进行结果持久化的写入
        ILineProcess<T> lineProcessor = processor == null ? null : processor.clone();
        // 持久化结果标识信息
        String newOrder = String.valueOf(order);
        IResultOutput<W> saver = getNewResultSaver(newOrder);
        executorPool.execute(() -> {
            try {
                String record = "order " + newOrder + ": " + lister.getPrefix();
                export(lister, saver, lineProcessor);
                record += "\tsuccessfully done";
                System.out.println(record);
                saver.closeWriters();
                if (lineProcessor != null) lineProcessor.closeResource();
                lister.close();
            } catch (Exception e) {
                System.out.println("order " + newOrder + ": " + lister.getPrefix() + "\tmarker: " +
                        lister.getMarker() + "\tend:" + lister.getEndPrefix());
                saver.closeWriters();
                if (lineProcessor != null) lineProcessor.closeResource();
                SystemUtils.exit(exitBool, e);
            }
        });
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

    /**
     * 生成 prefix 前缀下的列举对象
     * @param prefix 指定的前缀参数
     * @param marker 指定的列举开始 marker
     * @param end 指定的列举结束位置（文件名或文件名前缀）
     * @return 返回生成的范型列举对象
     * @throws SuitsException 生成列举对象失败抛出的异常
     */
    protected abstract ILister<E> getLister(String prefix, String marker, String end) throws SuitsException;

    private ILister<E> generateLister(String prefix) throws SuitsException {
        int retry = retryTimes;
        String[] markerAndEnd = getMarkerAndEnd(prefix);
        while (true) {
            try {
                return getLister(prefix, markerAndEnd[0], markerAndEnd[1]);
            } catch (SuitsException e) {
                System.out.println("generate lister by prefix:" + prefix + " retrying...\n" + e.getMessage());
                if (HttpRespUtils.checkStatusCode(e.getStatusCode()) < 0) throw e;
                else if (retry <= 0 && e.getStatusCode() >= 500) throw e;
                else retry--;
            }
        }
    }

    private String computePoint(ILister<E> lister, boolean doFutureCheck) throws SuitsException {
        boolean next;
        int retry = retryTimes;
        // 如果 endKey 为空的话表明 lister 没有后续的列表可以列举
        while (true) {
            try {
                next = doFutureCheck ? lister.hasFutureNext() : lister.hasNext();
                break;
            } catch (SuitsException e) {
                System.out.println("check lister has future next retrying...\n" + e.getMessage());
                if (HttpRespUtils.checkStatusCode(e.getStatusCode()) < 0) throw e;
                else if (retry <= 0 && e.getStatusCode() >= 500) throw e;
                else retry--;
            }
        }
        String startPrefix = lister.getPrefix();
        String point = "";
        String endKey = lister.currentEndKey();
        if (next && endKey != null) { // 如果存在 next 且当前获取的最后一个对象文件名不为空，则可以根据最后一个对象的文件名计算后续的前缀字符
            int prefixLen = startPrefix.length();
            // 如果最后一个对象的文件名长度大于 prefixLen，则可以取出从当前前缀开始的下一个字符，用于和预定义前缀列表进行比较，确定 lister 的
            // endPrefix
            if (endKey.length() > prefixLen) {
                point = endKey.substring(prefixLen, prefixLen + 1);
                // 如果此时下一个字符比预定义的最后一个前缀大的话（如中文文件名的情况）说明后续根据预定义前缀再检索无意义，则直接返回即可
                if (point.compareTo(originPrefixList.get(originPrefixList.size() - 1)) > 0) {
                    lister.setStraight(true);
                    // 如果 point 比第一个预定义前缀小则设置 lister 的结束位置到第一个预定义前缀，并且加入 lister 到返回的列举对象集
                } else if (point.compareTo(originPrefixList.get(0)) < 0) {
                    lister.setEndPrefix(startPrefix + originPrefixList.get(0));
                } else {
                    if (!prefixesMap.containsKey(startPrefix + point))
                        prefixesMap.put(startPrefix + point, new String[]{lister.getMarker(), ""});
                    lister.setEndPrefix(endKey);
                }
            } else {
                // 正常情况下存在 next 时最后一个文件对象不为空且其文件名不应为空，假如发生此情况直接将 lister 的结束位置设置到第一个预定义前
                // 缀并加入返回的对象集
                lister.setEndPrefix(startPrefix + originPrefixList.get(0));
            }
        } else {
            lister.setStraight(true);
        }
        return point;
    }

    private List<ILister<E>> nextLevelLister(List<String> prefixes, String startPrefix, String point) throws SuitsException {
        List<ILister<E>> nextLevelList = new ArrayList<>();
        for (String prefix : prefixes) {
            if (prefix.compareTo(point) >= 0 && checkPrefix(prefix)) {
                ILister<E> generated = generateLister(startPrefix + prefix);
                if (generated != null && generated.currentEndKey() != null) nextLevelList.add(generated);
            }
        }
        return nextLevelList;
    }

    private List<ILister<E>> parallelNextLevelLister(List<String> prefixes, String startPrefix, String point) {
        return prefixes.parallelStream()
                .filter(prefix -> prefix.compareTo(point) >= 0 && checkPrefix(prefix))
                .map(prefix -> {
                    try {
                        return generateLister(startPrefix + prefix);
                    } catch (SuitsException e) {
                        SystemUtils.exit(exitBool, e);
                        return null;
                    }
                }).collect(Collectors.toList());
    }

    private List<ILister<E>> filteredNextList(ILister<E> lister, AtomicInteger atomicOrder) throws Exception {
        String point = computePoint(lister, true);
        List<ILister<E>> nextList = lister.getStraight() ? new ArrayList<ILister<E>>(){{ add(lister); }} :
                nextLevelLister(originPrefixList, lister.getPrefix(), point);
        Iterator<ILister<E>> it = nextList.iterator();
        int size = nextList.size();
        // 为了更优的列举性能，考虑将每个 prefix 下一级迭代过程中产生的部分 lister 先执行，因为产生的下级列举对象本身是按前
        // 缀有序的，故保留最后一个不做执行，用于返回到汇总的列表中判断最后一个列举对象是否需要更新
        while (it.hasNext() && size > 1) {
            size--;
            ILister<E> eiLister = it.next();
            if(eiLister.canStraight()) {
                execInThread(eiLister, atomicOrder.addAndGet(1));
                it.remove();
            }
        }
        return nextList;
    }

    private int obtainThreadsToRun(List<ILister<E>> listerList, int order, String lastPrefix) throws Exception {
        AtomicInteger atomicOrder = new AtomicInteger(order);
        AtomicBoolean lastListerUpdated = new AtomicBoolean(false);
        while (listerList.size() > 0 && listerList.size() < threads) {
            // 是否更新了列举的末尾设置，每个 startLister 只需要更新一次末尾设置
            if (!lastListerUpdated.get()) {
//            if (!lastUpdated) {
//                ILister<E> lastLister =
                listerList.parallelStream().max(Comparator.comparing(ILister::getPrefix))
//                        .get();
                        .ifPresent(lastLister -> {
                            System.out.println("lastLister: " + lastLister.getPrefix() + "\t" + lastLister.hasNext());
                            // 得到计算后的最后一个列举对象，如果不存在 next 则说明该对象是下一级的末尾（最靠近结束位置）列举对象，更新其末尾设置
                            if (!lastLister.hasNext()) {
                                // 全局结尾则设置前缀为空，否则设置前缀为起始值
                                lastLister.setPrefix(lastPrefix);
                                lastLister.updateMarkerBy(lastLister.currentLast());
                                lastLister.setStraight(true);
                                lastListerUpdated.set(true);
//                        lastUpdated = true;
                            }
                        });
            }
            listerList = listerList.parallelStream().filter(eiLister -> {
                if (eiLister.canStraight()) {
                    try {
                        execInThread(eiLister, atomicOrder.addAndGet(1));
                    } catch (Exception e) {
                        SystemUtils.exit(exitBool, e);
                    }
                    return false;
                } else {
                    return true; // 对非 canStraight 的列举对象进行下一级的检索，得到更深层次前缀的可并发列举对象
                }
            }).map(lister -> {
                try {
                    return filteredNextList(lister, atomicOrder);
                } catch (Exception e) {
                    SystemUtils.exit(exitBool, e); return null;
                }
            }).filter(Objects::nonNull).reduce((list1, list2) -> { list1.addAll(list2); return list1; }).get();
        }

        // 如果末尾的 lister 尚未更新末尾设置则需要对此时的最后一个列举对象进行末尾设置的更新
        if (!lastListerUpdated.get()) {
//        if (!lastUpdated) {
            listerList.parallelStream().max(Comparator.comparing(ILister::getPrefix)).ifPresent(lister -> {
                lister.setPrefix(lastPrefix);
                if (!lister.hasNext()) lister.updateMarkerBy(lister.currentLast());
            });
        }
        for (ILister<E> lister : listerList) execInThread(lister, atomicOrder.addAndGet(1));
        return atomicOrder.get();
    }

    /**
     * 根据 startLister 得到可并发的下一级 lister 对象集放入多线程执行列举
     * @param startLister 已初始化的起始的 lister
     * @param globalEnd startLister 是否需要列举到全局的结尾处（从该 startLister 开始列举到整个空间结束）
     * @param order lister 执行的起始序号
     * @return 此次计算并执行到的 lister 序号，用于后续可能继续向线程添加 lister 执行设置起始序号
     * @throws Exception 下一级 lister 列表计算和多线程执行过程中可能产生的异常
     */
    private int computeToList(ILister<E> startLister, boolean globalEnd, int order) throws Exception {
        List<ILister<E>> listerList = null;
        if (threads > 1) {
            String point = computePoint(startLister, false);
            if (!startLister.getStraight()) {
                listerList = parallelNextLevelLister(originPrefixList, startLister.getPrefix(), point);
            }
        }
        if (listerList == null) {
            if (globalEnd) startLister.setPrefix("");
            if (!startLister.hasNext()) startLister.updateMarkerBy(startLister.currentLast());
            execInThread(startLister, order++);
            return order;
        } else {
            return obtainThreadsToRun(listerList, order, globalEnd ? "" : startLister.getPrefix());
        }
    }

    /**
     * 根据当前参数值创建多线程执行数据源导出工作
     */
    public void export() {
        String info = "list objects from bucket: " + bucket + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " running...");
        int order = 1;
        executorPool = Executors.newFixedThreadPool(threads);
        exitBool = new AtomicBoolean(false);
        try {
            if (prefixes == null || prefixes.size() == 0) {
                ILister<E> startLister = generateLister("");
                computeToList(startLister, true, order);
            } else {
                Collections.sort(prefixes);
                if (prefixLeft) {
                    ILister<E> startLister = generateLister("");
                    startLister.setEndPrefix(prefixes.get(0));
                    execInThread(startLister, order++);
                }
                for (int i = 0; i < prefixes.size() - 1; i++) {
                    ILister<E> startLister = generateLister(prefixes.get(i));
                    order = computeToList(startLister, false, order);
                }
                ILister<E> startLister = generateLister(prefixes.get(prefixes.size() - 1));
                if (prefixRight) {
                    computeToList(startLister, true, order);
                } else {
                    computeToList(startLister, false, order);
                }
            }
            executorPool.shutdown();
            while (!executorPool.isTerminated()) Thread.sleep(1000);
            System.out.println(info + " finished");
        } catch (Throwable throwable) {
            SystemUtils.exit(exitBool, throwable);
        }
    }
}
