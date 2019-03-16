package com.qiniu.service.datasource;

import com.qiniu.common.QiniuException;
import com.qiniu.persistence.FileMap;
import com.qiniu.service.convert.FileInfoToMap;
import com.qiniu.service.convert.MapToString;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.service.qoss.FileLister;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ListBucket implements IDataSource {

    final private Auth auth;
    final private Configuration configuration;
    final private String bucket;
    final private int threads;
    final private int unitLen;
    final private List<String> prefixes;
    final private Map<String, String[]> prefixesMap;
    final private List<String> antiPrefixes;
    final private boolean prefixLeft;
    final private boolean prefixRight;
    final private String savePath;
    private boolean saveTotal;
    private String saveFormat;
    private String saveSeparator;
    private List<String> rmFields;
    private ExecutorService executorPool; // 线程池
    private AtomicBoolean exitBool; // 多线程的原子操作 bool 值
    private ILineProcess<Map<String, String>> processor; // 定义的资源处理器

    public ListBucket(Auth auth, Configuration configuration, String bucket, int threads, int unitLen,
                      Map<String, String[]> prefixesMap, List<String> antiPrefixes, boolean prefixLeft,
                      boolean prefixRight, String savePath) {
        this.auth = auth;
        this.configuration = configuration;
        this.bucket = bucket;
        this.threads = threads;
        this.unitLen = unitLen;
        // 先设置 antiPrefixes 后再设置 prefixes，因为可能需要从 prefixes 中去除 antiPrefixes 含有的元素
        this.antiPrefixes = antiPrefixes == null ? new ArrayList<>() : antiPrefixes;
        this.prefixesMap = prefixesMap == null ? new HashMap<>() : prefixesMap;
        this.prefixes = prefixesMap == null ? new ArrayList<>() : removeAntiPrefixes(new ArrayList<String>(){{
            addAll(prefixesMap.keySet());
        }});
        this.prefixLeft = prefixLeft;
        this.prefixRight = prefixRight;
        this.savePath = savePath;
        this.saveTotal = false;
    }

    public void setResultOptions(boolean saveTotal, String format, String separator, List<String> rmFields) {
        this.saveTotal = saveTotal;
        this.saveFormat = format;
        this.saveSeparator = separator;
        this.rmFields = rmFields;
    }

    public void setProcessor(ILineProcess<Map<String, String>> processor) {
        this.processor = processor;
    }

    /**
     * 执行列举操作，直到当前的 FileLister 列举结束，并使用 processor 对象执行处理过程
     * @param fileLister 已经初始化的 FileLister 对象
     * @param fileMap 用于列举结果持久化的文件对象
     * @param processor 用于资源处理的处理器对象
     * @throws IOException 列举出现错误或者持久化错误抛出的异常
     */
    private void export(FileLister fileLister, FileMap fileMap, ILineProcess<Map<String, String>> processor)
            throws IOException {
        ITypeConvert<FileInfo, Map<String, String>> typeConverter = new FileInfoToMap();
        ITypeConvert<Map<String, String>, String> writeTypeConverter = new MapToString(saveFormat, saveSeparator, rmFields);
        List<FileInfo> fileInfoList;
        List<Map<String, String>> infoMapList;
        List<String> writeList;
        while (fileLister.hasNext()) {
            fileInfoList = fileLister.next();
            while (fileLister.exception != null) {
                System.out.println("list prefix:" + fileLister.getPrefix() + " retrying...");
                HttpResponseUtils.processException(fileLister.exception, 1, fileMap, new ArrayList<String>(){{
                    add(fileLister.getPrefix() + "|" + fileLister.getMarker());
                }});
                fileLister.exception = null;
                fileInfoList = fileLister.next();
            }
            infoMapList = typeConverter.convertToVList(fileInfoList);
            if (typeConverter.getErrorList().size() > 0)
                fileMap.writeError(String.join("\n", typeConverter.consumeErrorList()), false);
            if (saveTotal) {
                writeList = writeTypeConverter.convertToVList(infoMapList);
                if (writeList.size() > 0) fileMap.writeSuccess(String.join("\n", writeList), false);
            }
            // 如果抛出异常需要检测下异常是否是可继续的异常，如果是程序可继续的异常，忽略当前异常保持数据源读取过程继续进行
            try {
                if (processor != null) processor.processLine(infoMapList);
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, 1, null, null);
            }
        }
    }

    /**
     * 在 prefixes map 的参数配置中取出 marker 和 end 参数
     * @param prefix 配置的前缀参数
     * @return 返回针对该前缀配置的 marker 和 end
     */
    private String[] getMarkerAndEnd(String prefix) {
        String[] mapValue = prefixesMap.get(prefix);
        if (mapValue == null) return new String[]{"", ""};
        else return mapValue;
    }

    /**
     * 指定前缀得到 FileLister 列举器，预期异常情况下不断重试直到成功为止
     * @param prefix 指定的前缀参数
     * @return 返回得到的 FileLister
     * @throws IOException 如果出现非预期异常导致列举失败（初始化 FileLister）则抛出异常
     */
    private FileLister generateLister(String prefix) throws IOException {
        FileLister fileLister = null;
        boolean retry = true;
        while (retry) {
            try {
                fileLister = new FileLister(new BucketManager(auth, configuration), bucket, prefix,
                        getMarkerAndEnd(prefix)[0], getMarkerAndEnd(prefix)[1], null, unitLen);
                retry = false;
            } catch (QiniuException e) {
                System.out.println("list prefix:" + prefix + "\tmay be retrying...");
                HttpResponseUtils.processException(e, 1, null, null);
            }
        }
        return fileLister;
    }

    /**
     * 根据前缀列表得到 FileLister 列表，如果某个前缀列举失败抛出非预期异常直接终止程序
     * @param prefixList 指定的前缀列表参数
     * @return 返回得到的 FileLister 列表
     */
    private List<FileLister> prefixList(List<String> prefixList) {
        return prefixList.parallelStream()
                .map(prefix -> {
                    try {
                        return generateLister(prefix);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .filter(fileLister -> fileLister != null && fileLister.hasNext())
                .collect(Collectors.toList());
    }

    /**
     * 剔除不进行列举的 antiPrefixes 前缀列表
     * @param validPrefixList 过滤之前的 prefix 列表
     * @return 过滤之后的 prefix 列表
     */
    private List<String> removeAntiPrefixes(List<String> validPrefixList) {
        return validPrefixList.stream().filter(validPrefix -> {
            for (String antiPrefix : antiPrefixes) {
                if (validPrefix.startsWith(antiPrefix)) return false;
            }
            return true;
        }).collect(Collectors.toList());
    }

    /**
     * 通过 FileLister 得到目前列举出的最大文件名首字母和前缀列表进行比较，筛选出在当前列举位置之后的单字母前缀列表
     * 该方法是为了优化前缀检索过程中算法复杂度，通过剔除在当前列举位置之前的前缀，减少不必要的检索
     * @param fileLister 当前的 fileLister 参数，不能为空
     * @param originPrefixList 进行比较的初始前缀列表，不能为空
     * @return 返回比较之后的前缀列表
     */
    private List<String> nextPrefixes(FileLister fileLister, List<String> originPrefixList) {
        List<FileInfo> list = fileLister.getFileInfoList();
        String point = "";
        int prefixLen = fileLister.getPrefix().length();
        if (list != null && list.size() > 0) {
            String key0 = list.get(0).key;
            if (key0.length() > prefixLen + 1) point = key0.substring(prefixLen, prefixLen + 1);
            else if (key0.length() > prefixLen) point = key0.substring(prefixLen);
        }
        String finalPoint = point;
        return originPrefixList.stream()
                .filter(originPrefix -> originPrefix.compareTo(finalPoint) >= 0)
                .map(originPrefix -> fileLister.getPrefix() + originPrefix).collect(Collectors.toList());
    }

    /**
     * 添加第一段 FileLister 并设置结束标志 EndKeyPrefix，及为最后一段 FileLister 修改前缀 prefix 和开始 marker
     * 该方法是为了确保实际文件名不包含在预定义的前缀情况下的文件被成功列举（最大的预定义前缀之后的文件和最小的预定义前缀之前的文件）
     * @param fileListerList 通过计算得到的所有确定前缀下的列举器列表
     * @param customPrefix 计算该列举器列表时所用的前缀
     * @throws IOException 获取第一段未知前缀的列举器（generateLister(customPrefix)）时可能会抛出失败的异常
     */
    private void updateFileLiters(List<FileLister> fileListerList, String customPrefix) throws IOException {
        if (fileListerList.size() > 1) {
            fileListerList.add(generateLister(customPrefix));
            fileListerList.sort(Comparator.comparing(FileLister::getPrefix));
            fileListerList.get(0).setEndKeyPrefix(fileListerList.get(1).getPrefix());
            FileLister fileLister = fileListerList.get(fileListerList.size() -1);
            fileLister.setPrefix(customPrefix);
            if (!fileLister.checkMarkerValid()) {
                FileInfo lastFileInfo = fileLister.getFileInfoList().parallelStream().filter(Objects::nonNull)
                        .max(Comparator.comparing(fileInfo -> fileInfo.key)).orElse(null);
                fileLister.setMarker(ListBucketUtils.calcMarker(lastFileInfo));
            }
        }
    }

    /**
     * 计算所需要的 FileLiter 列表
     * @param customPrefix 定义的前缀
     * @return 根据线程数计算得到的 customPrefix 前缀下能够进行同时列举的 FileLiter 列表
     * @throws IOException 更新 FileLiter 列表可能产生的异常
     */
    private List<FileLister> computeFileLister(String customPrefix) throws IOException {
        List<String> originPrefixList = new ArrayList<>();
        // 由于目前指定包含 "|" 字符的前缀列举会导致超时，因此先将该字符包括 "{" 及其 ASCII 顺序之后的字符去掉（"|}~"），从而
        // 优化列举的超时问题，简化前缀参数的设置，也避免为了兼容该字符去修改代码算法
        // 去除前数个非常见作为文件名的 ASCII 字符（" !"#$%&'()*+,-"）优化前缀列举
        originPrefixList.addAll(Arrays.asList(("./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRST").split("")));
        originPrefixList.addAll(Arrays.asList(("UVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz").split("")));
        List<String> validPrefixList = new ArrayList<String>(){{add(customPrefix);}};
        List<FileLister> progressiveList = prefixList(validPrefixList);
        List<FileLister> fileListerList = new ArrayList<>();
        // 避免重复生成新对象，将 groupedListerMap 放在循环外部
        Map<Boolean, List<FileLister>> groupedListerMap;
        int size = progressiveList.size() + 1;
        while (size > 0 && size < threads) {
            // 给 progressiveList 按照是否有下一个 marker 进行分组，有下个 marker 的对象进一步进行前缀检索查询，没有下个 marker 的对象
            // 先添加进列表，整个列表 size 达到线程个数时即可放入线程池进行并发列举
            groupedListerMap = progressiveList.stream().collect(Collectors.groupingBy(FileLister::checkMarkerValid));
            if (groupedListerMap.get(false) != null) fileListerList.addAll(groupedListerMap.get(false));
            if (groupedListerMap.get(true) != null) {
                Optional<List<String>> listOptional = groupedListerMap.get(true).parallelStream()
                        .map(fileLister -> nextPrefixes(fileLister, originPrefixList))
                        .reduce((list1, list2) -> { list1.addAll(list2); return list1; });
                if (listOptional.isPresent() && listOptional.get().size() > 0) {
                    validPrefixList = removeAntiPrefixes(listOptional.get());
                    progressiveList = prefixList(validPrefixList);
                    size = fileListerList.size() + progressiveList.size() + 1;
                } else {
                    progressiveList = groupedListerMap.get(true);
                    break;
                }
            } else {
                progressiveList = new ArrayList<>();
                break;
            }
        }
        fileListerList.addAll(progressiveList);
        // 必须对计算得到的列举器列表进行更新
        updateFileLiters(fileListerList, customPrefix);
        return fileListerList;
    }

    /**
     * 从 FileLister 列表中取出对应的 FileLister 放入线程池中调用导出方法执行数据源数据导出工作，并可能进行 process 过程，记录导出结果
     * @param fileListerList 计算好的 FileLister 列表
     * @param recordFileMap 用于记录导出结果的持久化文件对象
     * @param order 目前执行的进度，已经执行多少个 FileLister 的列举
     * @throws Exception 持久化失败、列举失败等情况可能产生的异常
     */
    private void execInThreads(List<FileLister> fileListerList, FileMap recordFileMap, int order) throws Exception {
        for (int j = 0; j < fileListerList.size(); j++) {
            FileLister fileLister = fileListerList.get(j);
            // 如果是第一个线程直接使用初始的 processor 对象，否则使用 clone 的 processor 对象
            ILineProcess<Map<String, String>> lineProcessor = processor == null ? null :
                    order == 0 ? processor : processor.clone();
            // 持久化结果标识信息
            String identifier = String.valueOf(j + 1 + order);
            executorPool.execute(() -> {
                try {
                    FileMap fileMap = new FileMap(savePath, "listbucket", identifier);
                    fileMap.initDefaultWriters();
                    String record = "order " + identifier + ": " + fileLister.getPrefix();
                    recordFileMap.writeKeyFile("result", record + "\tlisting...", true);
                    export(fileLister, fileMap, lineProcessor);
                    record += "\tsuccessfully done";
                    System.out.println(record);
                    recordFileMap.writeKeyFile("result", record, true);
                    fileMap.closeWriters();
                    if (lineProcessor != null) lineProcessor.closeResource();
                    fileLister.remove();
                } catch (Exception e) {
                    System.out.println("marker: " + fileLister.getMarker() + "\tend:" + fileLister.getEndKeyPrefix());
                    SystemUtils.exit(exitBool, e);
                }
            });
        }
    }

    /**
     * 启动多线程导出的方法，根据线程数自动执行多线程并发导出
     * @throws Exception 计算 FileLister 列表失败或者写入失败等情况下的异常
     */
    public void export() throws Exception {
        String info = "list bucket" + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " running...");
        FileMap recordFileMap = new FileMap(savePath);
        executorPool = Executors.newFixedThreadPool(threads);
        exitBool = new AtomicBoolean(false);
        Collections.sort(prefixes);
        int alreadyOrder = 0;
        List<FileLister> fileListerList = new ArrayList<>();
        if (prefixes.size() == 0) {
            fileListerList = computeFileLister("");
            execInThreads(fileListerList, recordFileMap, alreadyOrder);
        } else {
            for (int i = 0; i < prefixes.size(); i++) {
                fileListerList.addAll(computeFileLister(prefixes.get(i)));
                // 第一个前缀可能需要判断是否列举该前缀排序之前的文件
                if (i == 0) {
                    if (prefixLeft) {
                        FileLister firstLister = generateLister("");
                        firstLister.setEndKeyPrefix(prefixes.get(0));
                        fileListerList.add(firstLister);
                    }
                } else if (i == prefixes.size() - 1) {
                    // 最后一个前缀可能需要判断是否列举该前缀排序之后的文件
                    if (fileListerList.size() > 1) {
                        fileListerList.sort(Comparator.comparing(FileLister::getPrefix));
                        if (prefixRight) fileListerList.get(fileListerList.size() -1).setPrefix("");
                    }
                }
                if (fileListerList.size() >= threads) {
                    execInThreads(fileListerList, recordFileMap, alreadyOrder);
                    alreadyOrder += fileListerList.size();
                    fileListerList = new ArrayList<>();
                }
            }
            execInThreads(fileListerList, recordFileMap, alreadyOrder);
        }
        executorPool.shutdown();
        recordFileMap.writeKeyFile("count_" + (alreadyOrder + fileListerList.size()), null, false);
        while (!executorPool.isTerminated()) Thread.sleep(1000);
        recordFileMap.closeWriters();
        System.out.println(info + " finished");
    }
}
