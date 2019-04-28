package com.qiniu.datasource;

import com.qiniu.common.QiniuException;
import com.qiniu.common.SuitsException;
import com.qiniu.entry.CommonParams;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.IResultSave;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.LineUtils;
import com.qiniu.util.SystemUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class OssContainer<E, W> implements IDataSource<ILister<E>, IResultSave<W>> {

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
    protected List<String> rmFields;
    private ExecutorService executorPool; // 线程池
    private AtomicBoolean exitBool; // 多线程的原子操作 bool 值
    private List<String> originPrefixList = new ArrayList<>();
    private ILineProcess<Map<String, String>> processor; // 定义的资源处理器

    public OssContainer(String bucket, List<String> antiPrefixes, Map<String, String[]> prefixesMap, boolean prefixLeft,
                        boolean prefixRight, Map<String, String> indexMap, int unitLen, int threads) {
        this.bucket = bucket;
        // 先设置 antiPrefixes 后再设置 prefixes，因为可能需要从 prefixes 中去除 antiPrefixes 含有的元素
        this.antiPrefixes = antiPrefixes;
        this.prefixesMap = prefixesMap;
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
    public void setSaveOptions(String savePath, boolean saveTotal, String format, String separator, List<String> rmFields) {
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
        this.bucket = commonParams.getBucket();
        this.antiPrefixes = commonParams.getAntiPrefixes();
        this.prefixesMap = commonParams.getPrefixesMap();
        setPrefixes();
        this.prefixLeft = commonParams.getPrefixLeft();
        this.prefixRight = commonParams.getPrefixRight();
        setIndexMapWithDefault(commonParams.getIndexMap());
        this.unitLen = commonParams.getUnitLen();
        this.retryTimes = commonParams.getRetryTimes();
        this.threads = commonParams.getThreads();
        this.savePath = commonParams.getSavePath();
        this.saveTotal = commonParams.getSaveTotal();
        this.saveFormat = commonParams.getSaveFormat();
        this.saveSeparator = commonParams.getSaveSeparator();
        this.rmFields = commonParams.getRmFields();
    }

    public void setProcessor(ILineProcess<Map<String, String>> processor) {
        this.processor = processor;
    }

    private void setPrefixes() {
        this.prefixes = new ArrayList<>();
        if (prefixesMap != null) {
            for (String prefix : prefixesMap.keySet()) {
                if (checkAntiPrefixes(prefix)) prefixes.add(prefix);
            }
        } else {
            prefixesMap = new HashMap<>() ;
        }
    }

    /**
     * 检验 prefix 是否在 antiPrefixes 前缀列表中
     * @param validPrefix 待检验的 prefix
     * @return 检验结果，true 表示 validPrefix 有效不需要剔除
     */
    private boolean checkAntiPrefixes(String validPrefix) {
        if (antiPrefixes == null) antiPrefixes = new ArrayList<>();
        for (String antiPrefix : antiPrefixes) {
            if (validPrefix.startsWith(antiPrefix)) return false;
        }
        return true;
    }

    protected abstract ITypeConvert<E, Map<String, String>> getNewMapConverter();

    protected abstract ITypeConvert<E, String> getNewStringConverter() throws IOException;

    /**
     * 执行列举操作，直到当前的 lister 列举结束，并使用 processor 对象执行处理过程
     * @param lister 已经初始化的 lister 对象
     * @param saver 用于列举结果持久化的文件对象
     * @param processor 用于资源处理的处理器对象
     * @throws IOException 列举出现错误或者持久化错误抛出的异常
     */
    public void export(ILister<E> lister, IResultSave<W> saver, ILineProcess<Map<String, String>> processor) throws IOException {
        ITypeConvert<E, Map<String, String>> mapConverter = getNewMapConverter();
        ITypeConvert<E, String> stringConverter = getNewStringConverter();
        List<E> objects;
        List<Map<String, String>> infoMapList;
        List<String> writeList;
        int retry;
        boolean goon = true;
        // 初始化的 lister 包含首次列举的结果列表，需要先取出，后续向前列举时会更新其结果列表
        do {
            objects = lister.currents();
            if (saveTotal) {
                writeList = stringConverter.convertToVList(objects);
                if (writeList.size() > 0) saver.writeSuccess(String.join("\n", writeList), false);
                if (stringConverter.errorSize() > 0)
                    saver.writeError(String.join("\n", stringConverter.consumeErrors()), false);
            }
            // 如果抛出异常需要检测下异常是否是可继续的异常，如果是程序可继续的异常，忽略当前异常保持数据源读取过程继续进行
            try {
                if (processor != null) {
                    infoMapList = mapConverter.convertToVList(objects);
                    if (mapConverter.errorSize() > 0)
                        saver.writeError(String.join("\n", mapConverter.consumeErrors()), false);
                    processor.processLine(infoMapList);
                }
            } catch (QiniuException e) {
                if (HttpResponseUtils.checkException(e, 2) < -1) throw e;
            }
            retry = retryTimes;
            while (true) {
                try {
                    // 如果存在后续则向前列举，否则使循环退出即可
                    if (lister.hasNext()) lister.listForward();
                    else goon = false;
                    break;
                } catch (SuitsException e) {
                    System.out.println("list objects by prefix:" + lister.getPrefix() + " retrying...\n" + e.getMessage());
                    if (HttpResponseUtils.checkStatusCode(e.getStatusCode()) < 0) throw e;
                    else if (retry <= 0 && e.getStatusCode() >= 500) throw e;
                    else retry--;
                }
            }
        } while (goon);
    }

    protected abstract IResultSave<W> getNewResultSaver(String order) throws IOException;

    /**
     * 将 ILister<E> 对象放入线程池进行执行列举，如果 processor 不为空则同时执行 process 过程
     * @param lister 列举对象
     * @param recordSaver 记录整体进度信息的文件对象
     * @param order 当前列举对象集的起始序号
     * @throws Exception 操作失败抛出的异常
     */
    public void execInThread(ILister<E> lister, IResultSave<W> recordSaver, int order) throws Exception {
        // 如果是第一个线程直接使用初始的 processor 对象，否则使用 clone 的 processor 对象，多线程情况下不要直接使用传入的 processor，
        // 因为对其关闭会造成 clone 的对象无法进行结果持久化的写入
        ILineProcess<Map<String, String>> lineProcessor = processor == null ? null : processor.clone();
        // 持久化结果标识信息
        String newOrder = String.valueOf(order);
        IResultSave<W> saver = getNewResultSaver(newOrder);
        executorPool.execute(() -> {
            try {
                String record = "order " + newOrder + ": " + lister.getPrefix();
                recordSaver.writeKeyFile("result", record + "\tlisting...", true);
                export(lister, saver, lineProcessor);
                record += "\tsuccessfully done";
                System.out.println(record);
                recordSaver.writeKeyFile("result", record, true);
                saver.closeWriters();
                if (lineProcessor != null) lineProcessor.closeResource();
                lister.close();
            } catch (Exception e) {
                System.out.println("order " + newOrder + ": " + lister.getPrefix() + "\tmarker: " +
                        lister.getMarker() + "\tend:" + lister.getEndPrefix());
                recordSaver.closeWriters();
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
        while (true) {
            try {
                String[] markerAndEnd = getMarkerAndEnd(prefix);
                return getLister(prefix, markerAndEnd[0], markerAndEnd[1]);
            } catch (SuitsException e) {
                System.out.println("generate lister by prefix:" + prefix + " retrying...\n" + e.getMessage());
                if (HttpResponseUtils.checkStatusCode(e.getStatusCode()) < 0) throw e;
                else if (retry <= 0 && e.getStatusCode() >= 500) throw e;
                else retry--;
            }
        }
    }

    /**
     * 从起始的 lister 对象来得到下一级别可并发的列举对象集
     * @param lister 起始列举对象
     * @return 下一级别可并发的列举对象集
     */
    private List<ILister<E>> nextLevelLister(ILister<E> lister, boolean doFutureCheck) throws SuitsException {
        int retry = retryTimes;
        boolean futureNext;
        while (true) {
            try {
                futureNext = doFutureCheck ? lister.hasFutureNext() : lister.hasNext();
                break;
            } catch (SuitsException e) {
                System.out.println("check lister has future next retrying...\n" + e.getMessage());
                if (HttpResponseUtils.checkStatusCode(e.getStatusCode()) < 0) throw e;
                else if (retry <= 0 && e.getStatusCode() >= 500) throw e;
                else retry--;
            }
        }
        String startPrefix = lister.getPrefix();;
        String point = "";
        List<ILister<E>> nextLevelList = new ArrayList<>();
        // 如果没有可继续的 marker 的话则不需要再往前进行检索了，直接返回仅包含该 lister 的列表
        if (!futureNext) {
//        if (!lister.hasNext()) {
            nextLevelList.add(lister);
            return nextLevelList;
        // 如果存在 next 且当前获取的最后一个对象不为空，则可以根据最后一个对象的文件名计算后续的前缀字符
        } else if (lister.currentLast() != null) {
            int prefixLen = startPrefix.length();
            String lastKey = lister.currentLastKey();
            // 如果最后一个对象的文件名长度大于 prefixLen，则可以取出从当前前缀开始的下一个字符，用于和预定义前缀列表进行比较，确定 lister 的
            // endPrefix
            if (lastKey != null && lastKey.length() > prefixLen) {
                point = lastKey.substring(prefixLen, prefixLen + 1);
                // 如果此时下一个字符比预定义的最后一个前缀大的话（如中文文件名的情况）说明后续根据预定义前缀再检索无意义，则直接返回即可
                if (point.compareTo(originPrefixList.get(originPrefixList.size() - 1)) > 0) {
                    lister.setStraight(true);
                    nextLevelList.add(lister);
                    return nextLevelList;
                // 如果 point 比第一个预定义前缀小则设置 lister 的结束位置到第一个预定义前缀，并且加入 lister 到返回的列举对象集
                } else if (point.compareTo(originPrefixList.get(0)) < 0) {
                    lister.setEndPrefix(startPrefix + originPrefixList.get(0));
                    nextLevelList.add(lister);
                } else {
                    if (!prefixesMap.containsKey(startPrefix + point))
                        prefixesMap.put(startPrefix + point, new String[]{lister.getMarker(), ""});
                    lister.setEndPrefix(lastKey);
                    nextLevelList.add(lister);
                }
            // 正常情况下文件对象不为空则其文件名不应为空，假如发生此情况直接将 lister 的结束位置设置到第一个预定义前缀并加入返回的对象集
            } else {
                lister.setEndPrefix(startPrefix + originPrefixList.get(0));
                nextLevelList.add(lister);
            }
        // 正常情况下存在 next 时最后一个对象不应为空，假如出现此情况时直接将 lister 的结束位置设置到第一个预定义前缀，并加入列举对象集
        } else {
            lister.setEndPrefix(startPrefix + originPrefixList.get(0));
            nextLevelList.add(lister);
        }

        for (String prefix : originPrefixList) {
            if (prefix.compareTo(point) >= 0 && checkAntiPrefixes(prefix)) {
                ILister<E> generated = generateLister(startPrefix + prefix);
                if (generated != null && generated.currentLast() != null) nextLevelList.add(generated);
            }
        }
        return nextLevelList;
    }

    /**
     * 根据 startLister 得到可并发的下一级 lister 对象集放入多线程执行列举
     * @param startLister 已初始化的起始的 lister
     * @param globalEnd startLister 是否需要列举到全局的结尾处（从该 startLister 开始列举到整个空间结束）
     * @param order lister 执行的起始序号
     * @param recordSaver 记录全局执行结果的文件持久化对象
     * @return 此次计算并执行到的 lister 序号，用于后续可能继续向线程添加 lister 执行设置起始序号
     * @throws Exception 下一级 lister 列表计算和多线程执行过程中可能产生的异常
     */
    private int computeToList(ILister<E> startLister, boolean globalEnd, int order, IResultSave<W> recordSaver)
            throws Exception {
        List<ILister<E>> listerList = nextLevelLister(startLister, false);
        boolean lastListerUpdated = false;
        ILister<E> lastLister;
        List<ILister<E>> forwardList = new ArrayList<>();
        Optional<List<ILister<E>>> optional;
        while (true) {
            // 是否更新了列举的末尾设置，每个 startLister 只需要更新一次末尾设置
            if (!lastListerUpdated) {
                lastLister = listerList.stream().max(Comparator.comparing(ILister::getPrefix)).orElse(null);
                // 得到计算后的最后一个列举对象，如果不存在 next 则说明该对象是下一级的末尾（最靠近结束位置）列举对象，更新其末尾设置
                if (lastLister != null && !lastLister.hasNext()) {
                    // 全局结尾则设置前缀为空，否则设置前缀为起始值
                    if (globalEnd) lastLister.setPrefix("");
                    else lastLister.setPrefix(startLister.getPrefix());
                    lastLister.updateMarkerBy(lastLister.currentLast());
                    lastLister.setStraight(true);
                    lastListerUpdated = true;
                }
            }
            // 按照 canStraight 来进行分组，将部分不需要向下分级的 lister 提前放入线程中执行列举
            for (ILister<E> eiLister : listerList) {
                if (eiLister.canStraight()) execInThread(eiLister, recordSaver, order++);
                else forwardList.add(eiLister);
            }
            // 对非 canStraight 的列举对象进行下一级的检索，得到更深层次前缀的可并发列举对象
            if (forwardList.size() > 0 && forwardList.size() < threads) {
                optional = forwardList.parallelStream().map(lister -> {
                    try {
                        return nextLevelLister(lister, true);
                    } catch (Exception e) {
                        SystemUtils.exit(exitBool, e); return null;
                    }
                }).filter(Objects::nonNull).reduce((list1, list2) -> { list1.addAll(list2); return list1; });
                if (optional.isPresent() && optional.get().size() > 0) {
                    listerList = optional.get();
                    forwardList.clear();
                } else {
                    listerList = forwardList; break;
                }
            } else {
                listerList = forwardList; break;
            }
        }

        if (listerList.size() > 0) {
            // 如果末尾的 lister 尚未更新末尾设置则需要对此时的最后一个列举对象进行末尾设置的更新
            if (!lastListerUpdated) {
                lastLister = listerList.stream().max(Comparator.comparing(ILister::getPrefix)).get();
                if (globalEnd) lastLister.setPrefix("");
                else lastLister.setPrefix(startLister.getPrefix());
                if (!lastLister.hasNext()) lastLister.updateMarkerBy(lastLister.currentLast());
            }
            for (ILister<E> lister : listerList) {
                execInThread(lister, recordSaver, order++);
            }
        }
        return order;
    }

    /**
     * 根据当前参数值创建多线程执行数据源导出工作
     * @throws Exception 数据源导出时出现错误抛出异常
     */
    public void export() throws Exception {
        String info = "list objects from bucket: " + bucket + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " running...");
        IResultSave<W> recordSaver = getNewResultSaver(null);
        int order = 1;
        exitBool = new AtomicBoolean(false);
        try {
            executorPool = Executors.newFixedThreadPool(threads);
            Collections.sort(prefixes);
            if (prefixes.size() == 0) {
                ILister<E> startLister = generateLister("");
                computeToList(startLister, true, order, recordSaver);
            } else {
                if (prefixLeft) {
                    ILister<E> startLister = generateLister("");
                    startLister.setEndPrefix(prefixes.get(0));
                    execInThread(startLister, recordSaver, order++);
                }
                for (int i = 0; i < prefixes.size() - 1; i++) {
                    ILister<E> startLister = generateLister(prefixes.get(i));
                    order = computeToList(startLister, false, order, recordSaver);
                }
                ILister<E> startLister = generateLister(prefixes.get(prefixes.size() - 1));
                if (prefixRight) {
                    computeToList(startLister, true, order, recordSaver);
                } else {
                    computeToList(startLister, false, order, recordSaver);
                }
            }
            executorPool.shutdown();
            while (!executorPool.isTerminated()) Thread.sleep(1000);
            recordSaver.closeWriters();
            System.out.println(info + " finished");
        } catch (Throwable throwable) {
            SystemUtils.exit(exitBool, throwable);
        }
    }
}
