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
    private List<String> originPrefixList = new ArrayList<>();
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
        // 由于目前指定包含 "|" 字符的前缀列举会导致超时，因此先将该字符及其 ASCII 顺序之前的 "{" 和之后的（"|}~"）统一去掉，从而优化列举的超
        // 时问题，简化前缀参数的设置，也避免为了兼容该字符去修改代码算法
        originPrefixList.addAll(Arrays.asList((" !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMN").split("")));
        originPrefixList.addAll(Arrays.asList(("OPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz").split("")));
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
     * 最后一段 FileLister 修改前缀 prefix 和下一个 marker，该方法是为了确保实际文件名前缀在最大的预定义前缀之后的文件被成功列举
     * @param lastLister 通过计算得到的所有确定前缀下的列举器列表
     * @param prefix 计算该列举器列表时所用的前缀
     */
    private void updateLastLiter(FileLister lastLister, String prefix) {
        if (lastLister != null) {
            int size = lastLister.getFileInfoList().size();
            lastLister.setPrefix(prefix);
            if (!lastLister.checkMarkerValid()) {
                // 实际上传过来的 FileLister 在下一个 marker 为空的情况下 FileInfoList 是应该一定包含数据的
                FileInfo lastFileInfo = size > 0 ? lastLister.getFileInfoList().get(size -1) : null;
                lastLister.setMarker(ListBucketUtils.calcMarker(lastFileInfo));
            }
        }
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
            FileMap fileMap = new FileMap(savePath, "listbucket", identifier);
            fileMap.initDefaultWriters();
            executorPool.execute(() -> {
                try {
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
                    System.out.println("order " + identifier + ": " + fileLister.getPrefix() + "\tmarker: " +
                            fileLister.getMarker() + "\tend:" + fileLister.getEndKeyPrefix());
                    recordFileMap.closeWriters();
                    fileMap.closeWriters();
                    SystemUtils.exit(exitBool, e);
                }
            });
        }
    }

    /**
     * 通过 FileLister 得到目前列举出的最大文件名首字母和前缀列表进行比较，筛选出在当前列举位置之后的单字母前缀列表，该方法是为了优化前缀检索过程
     * 中算法复杂度，通过剔除在当前列举位置之前的前缀，减少不必要的检索，如果传过来的 FileLister 的 FileInfoList 中没有数据的话应当是存在下一
     * 个 marker 的（可能是文件被删除的情况），也可以直接检索下一级前缀（所有的前缀字符都比 "" 大），对初始的列举器更新参数并对所有有效的下一级前
     * 缀进行检索并生成列举器列表
     * @param fileLister 当前的 fileLister 参数，不能为空
     * @return 根据检索结果得到的下一级列举器列表
     */
    private List<FileLister> nextLevelLister(FileLister fileLister) {
        // 由于下面 point 是通过最大的文件名来计算的，故初始的 FileLister 还是要放入列表中
        // 如果没有可继续的 marker 的话则不需要再往前进行检索了，直接返回仅包含该 fileLister 的列表
        List<FileLister> nextLevelList = new ArrayList<>();
        if (!fileLister.checkMarkerValid()) {
            nextLevelList.add(fileLister);
            return nextLevelList;
        }

        List<FileInfo> fileInfoList = fileLister.getFileInfoList();
        String point = "";
        int prefixLen = fileLister.getPrefix().length();
        if (fileInfoList != null && fileInfoList.size() > 0) {
            String keyLast = fileInfoList.get(fileInfoList.size() - 1).key;
            if (keyLast.length() > prefixLen + 1) point = keyLast.substring(prefixLen, prefixLen + 1);
            else if (keyLast.length() > prefixLen) point = keyLast.substring(prefixLen);
            // 如果得到的列表中第一个文件和最后一个文件的下一级前缀是相同的话，说明此次列举只有一个下级前缀，则不需要将此 fileLister
            // 添加进列表，反之则应该添加之列表中，后面还需要根据第一个（最小的）下一级前缀来设置 endKeyPrefix
            if (!fileInfoList.get(0).key.startsWith(keyLast.substring(0, prefixLen))) {
                nextLevelList.add(fileLister);
            }
        }
        String finalPoint = point;
        List<String> validPrefixList = originPrefixList.parallelStream()
                .filter(originPrefix -> originPrefix.compareTo(finalPoint) >= 0)
                .map(originPrefix -> fileLister.getPrefix() + originPrefix)
                .collect(Collectors.toList());

        // 列举到的位置已经超过了所有预定义前缀，直接返回即可
        if (validPrefixList.size() == 0) return nextLevelList;
        // 从上步得到的前缀列表都会进行下一级检索列举，初始的 fileLister 应当将第一个前缀作为结束符
        fileLister.setEndKeyPrefix(validPrefixList.get(0));
        // 去掉不进行列举的前缀
        validPrefixList = removeAntiPrefixes(validPrefixList);
        nextLevelList.addAll(prefixList(validPrefixList));
        return nextLevelList;
    }

    /**
     * 根据 startLister（及其 prefix 参数）和有序的前缀列表往前计算下级有效的前缀并得到新的 FileLister 放入线程执行列举
     * @param lastPrefix startLister 的 prefix 条件下能够得到的 FileLister 列表中最后一个 FileLister 需要更新的前缀
     * @return 此次计算并执行的 FileLister 个数，用于后续可能继续添加 FileLister 执行，编号需要进行递增
     * @throws IOException FileLister 列表放入线程执行过程中可能产生的异常
     */
    private int computeToList(FileLister startLister, String lastPrefix, int alreadyOrder, FileMap recordFileMap)
            throws Exception {
        List<FileLister> fileListerList = nextLevelLister(startLister);
        List<FileLister> execListerList = new ArrayList<>();
        boolean lastListerUpdated = false;
        FileLister lastLister;
        int nextSize = 0;
        // 避免重复生成新对象，将 groupedListerMap 放在循环外部
        Map<Boolean, List<FileLister>> groupedListerMap;
        do {
            // 取出前缀最大的 FileLister，如果其不存在下一个 marker，则不需要继续往下计算前缀，那么这将是整体的最后一个 FileLister，为了保
            // 证能列举到 lastPrefix 的所有文件，需要对其现有的 prefix 做更新，一般情况下 startLister.getPrefix() 是和 lastPrefix 相
            // 同的，但是如果初始参数 prefixes 不为空且 prefixRight 为 true 的情况下表示需要列举 prefixes 中最后一个前缀开始的文件之后的
            // 所有文件，则需要将此 FileLister 更新前缀为 lastPrefix
            if (!lastListerUpdated) {
                fileListerList.sort(Comparator.comparing(FileLister::getPrefix));
                lastLister = fileListerList.get(fileListerList.size() - 1);
                if (lastLister != null && !lastLister.checkMarkerValid()) {
                    updateLastLiter(lastLister, lastPrefix);
                    execListerList.add(lastLister);
                    lastListerUpdated = true;
                    // 由于 updateLastLiter 操作会更新 marker，避免下一步操作将 lastLister 再加入检索，将其独立处理，只对其他的进行分组
                    fileListerList = fileListerList.subList(0, fileListerList.size() -1 );
                }
            }
            // 给 progressiveList 按照是否有下一个 marker 进行分组，有下个 marker 的对象进一步进行前缀检索查询，没有下个 marker 的对象
            // 先添加进列表，整个列表 size 达到线程个数时即可放入线程池进行并发列举
            // 加入 !fileLister.checkEndKeyPrefixValid()) 过滤的原因是经过 nextLevelLister 处理时将初始列举器设置了 endKeyPrefix，
            // 需要将其直接放入线程执行不做进一步前缀检索
            groupedListerMap = fileListerList.parallelStream().collect(Collectors.groupingBy(fileLister ->
                    fileLister.checkMarkerValid() && !fileLister.checkEndKeyPrefixValid()));
            if (groupedListerMap.get(false) != null) execListerList.addAll(groupedListerMap.get(false));
            // 将没有下一个 marker 的 FileLister 先放入线程执行掉
            execInThreads(execListerList, recordFileMap, alreadyOrder);
            alreadyOrder += execListerList.size();
            execListerList.clear();
            // 有下一个有效 marker 的 FileLister 可以用于继续检索前缀
            if (groupedListerMap.get(true) != null) {
                Optional<List<FileLister>> listOptional = groupedListerMap.get(true).parallelStream()
                        .map(this::nextLevelLister)
                        .reduce((list1, list2) -> { list1.addAll(list2); return list1; });
                if (listOptional.isPresent() && listOptional.get().size() > 0) {
                    fileListerList = listOptional.get();
                    nextSize = fileListerList.parallelStream().filter(FileLister::checkMarkerValid)
                            .collect(Collectors.toList()).size();
                } else {
                    fileListerList = groupedListerMap.get(true);
                    break;
                }
            } else {
                fileListerList.clear();
                break;
            }
        } while (nextSize < threads);

        // 如果没有更新过整体的最后一个 FileLister 则必须对计算得到的列举器列表中最大的一个 FileLister 进行更新
        if (!lastListerUpdated && fileListerList.size() > 0) {
            fileListerList.sort(Comparator.comparing(FileLister::getPrefix));
            lastLister = fileListerList.get(fileListerList.size() - 1);
            updateLastLiter(lastLister, lastPrefix);
        }
        execInThreads(fileListerList, recordFileMap, alreadyOrder);
        alreadyOrder += fileListerList.size();
        return alreadyOrder;
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
        if (prefixes.size() == 0) {
            FileLister startLister = generateLister("");
            computeToList(startLister, "", alreadyOrder, recordFileMap);
        } else {
            if (prefixLeft) {
                FileLister startLister = generateLister("");
                startLister.setEndKeyPrefix(prefixes.get(0));
                execInThreads(new ArrayList<FileLister>(){{ add(startLister); }}, recordFileMap, alreadyOrder);
                alreadyOrder += 1;
            }
            for (int i = 0; i < prefixes.size() - 1; i++) {
                FileLister startLister = generateLister(prefixes.get(i));
                alreadyOrder = computeToList(startLister, prefixes.get(i), alreadyOrder, recordFileMap);
            }
            FileLister startLister = generateLister(prefixes.get(prefixes.size() - 1));
            if (prefixRight) {
                computeToList(startLister, "", alreadyOrder, recordFileMap);
            } else {
                computeToList(startLister, prefixes.get(prefixes.size() - 1), alreadyOrder, recordFileMap);
            }
        }
        executorPool.shutdown();
        while (!executorPool.isTerminated()) Thread.sleep(1000);
        recordFileMap.closeWriters();
        System.out.println(info + " finished");
    }
}
