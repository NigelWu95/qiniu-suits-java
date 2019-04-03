package com.qiniu.datasource;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.model.COSObjectSummary;
import com.qiniu.common.QiniuException;
import com.qiniu.convert.FileInfoToMap;
import com.qiniu.convert.FileInfoToString;
import com.qiniu.entry.CommonParams;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileMap;
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

public class TenObjectsList implements IDataSource {

    private String secretId;
    private String secretKey;
    private ClientConfig clientConfig;
    private String bucket;
    private List<String> antiPrefixes;
    private Map<String, String[]> prefixesMap;
    private List<String> prefixes;
    private boolean prefixLeft;
    private boolean prefixRight;
    private Map<String, String> indexMap;
    private int unitLen;
    private int threads;
    private int retryTimes = 5;
    private String savePath;
    private boolean saveTotal;
    private String saveFormat;
    private String saveSeparator;
    private List<String> rmFields;
    private ExecutorService executorPool; // 线程池
    private AtomicBoolean exitBool; // 多线程的原子操作 bool 值
    private List<String> originPrefixList = new ArrayList<>();
    private ILineProcess<Map<String, String>> processor; // 定义的资源处理器

    public TenObjectsList(String secretId, String secretKey, ClientConfig clientConfig, String bucket,
                          List<String> antiPrefixes, Map<String, String[]> prefixesMap, boolean prefixLeft, boolean prefixRight,
                          Map<String, String> indexMap, int unitLen, int threads, String savePath) {
        this.secretId = secretId;
        this.secretKey = secretKey;
        this.clientConfig = clientConfig;
        this.bucket = bucket;
        // 先设置 antiPrefixes 后再设置 prefixes，因为可能需要从 prefixes 中去除 antiPrefixes 含有的元素
        this.antiPrefixes = antiPrefixes == null ? new ArrayList<>() : antiPrefixes;
        this.prefixesMap = prefixesMap == null ? new HashMap<>() : prefixesMap;
        setPrefixes();
        this.prefixLeft = prefixLeft;
        this.prefixRight = prefixRight;
        setIndexMapWithDefault(indexMap);
        this.unitLen = unitLen;
        this.threads = threads;
        this.savePath = savePath;
        this.saveTotal = true; // 默认全记录保存
        // 由于目前指定包含 "|" 字符的前缀列举会导致超时，因此先将该字符及其 ASCII 顺序之前的 "{" 和之后的（"|}~"）统一去掉，从而优化列举的超
        // 时问题，简化前缀参数的设置，也避免为了兼容该字符去修改代码算法
        originPrefixList.addAll(Arrays.asList((" !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMN").split("")));
        originPrefixList.addAll(Arrays.asList(("OPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz").split("")));
    }

    // 不调用则各参数使用默认值
    public void setSaveOptions(boolean saveTotal, String format, String separator, List<String> rmFields) {
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
        }
    }
    /**
     * 执行列举操作，直到当前的 FileLister 列举结束，并使用 processor 对象执行处理过程
     * @param tenLister 已经初始化的 TenLister 对象
     * @param fileMap 用于列举结果持久化的文件对象
     * @param processor 用于资源处理的处理器对象
     * @throws IOException 列举出现错误或者持久化错误抛出的异常
     */
    private void export(TenLister tenLister, FileMap fileMap, ILineProcess<Map<String, String>> processor)
            throws IOException {
        ITypeConvert<FileInfo, Map<String, String>> typeConverter = new FileInfoToMap(indexMap);
        ITypeConvert<FileInfo, String> writeTypeConverter = new FileInfoToString(saveFormat, saveSeparator, rmFields);
        List<COSObjectSummary> cosObjectSummaries = null;
        List<Map<String, String>> infoMapList = null;
        List<String> writeList = new ArrayList<>();
        int statusCode;
        int retry;
        while (tenLister.hasNext()) {
            retry = retryTimes + 1;
            while (retry > 0) {
                cosObjectSummaries = tenLister.next();
                statusCode = tenLister.getStatusCode();
                if (tenLister.getError() != null) {
                    System.out.println("list prefix:" + tenLister.getPrefix() + " retrying...");
                    retry--;
                    // 如果状态码检测小于 0 则抛出异常，否则重试，当重试次数为 0 同时状态码为 599 则会抛出异常
                    if (HttpResponseUtils.checkStatusCode(statusCode) < 0 || (retry == 0 && statusCode == 599))
                        throw new IOException(tenLister.getError());
                } else {
                    retry = 0;
                }
            }

//            infoMapList = typeConverter.convertToVList(cosObjectSummaries);
            if (typeConverter.getErrorList().size() > 0)
                fileMap.writeError(String.join("\n", typeConverter.consumeErrorList()), false);
            if (saveTotal) {
//                writeList = writeTypeConverter.convertToVList(cosObjectSummaries);
                if (writeList.size() > 0) fileMap.writeSuccess(String.join("\n", writeList), false);
                if (writeTypeConverter.getErrorList().size() > 0)
                    fileMap.writeError(String.join("\n", writeTypeConverter.consumeErrorList()), false);
            }
            // 如果抛出异常需要检测下异常是否是可继续的异常，如果是程序可继续的异常，忽略当前异常保持数据源读取过程继续进行
            try {
                if (processor != null) processor.processLine(infoMapList);
            } catch (QiniuException e) {
                // 这里其实逻辑上没有做重试次数的限制，因为返回的 retry 始终大于等于 -1，除非是 process 出现 599 状态码才会抛出异常
                retry = HttpResponseUtils.checkException(e, 1);
                if (retry == -2) throw e;
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

    /**
     * 指定前缀得到 FileLister 列举器，预期异常情况下不断重试直到成功为止
     * @param prefix 指定的前缀参数
     * @return 返回得到的 FileLister
     * @throws IOException 如果出现非预期异常导致列举失败（初始化 FileLister）则抛出异常
     */
    private TenLister generateLister(String prefix) throws IOException {
        TenLister tenLister;
        int retry = retryTimes + 1;
        while (true) {
            try {
                tenLister = new TenLister(new COSClient(new BasicCOSCredentials(secretId, secretKey), clientConfig),
                        bucket, prefix, getMarkerAndEnd(prefix)[0], getMarkerAndEnd(prefix)[1], null, unitLen);
                break;
            } catch (CosClientException e) {
                System.out.println("list prefix:" + prefix + "\tmay be retrying...");
//                retry = HttpResponseUtils.checkException(e, retry);
                if (retry == -2) throw e; // 只有当重试次数用尽且响应状态码为 599 时才会抛出异常
            }
        }
        return tenLister;
    }

    /**
     * 检验 prefix 是否在 antiPrefixes 前缀列表中
     * @param validPrefix 待检验的 prefix
     * @return 检验结果，true 表示 validPrefix 有效不需要剔除
     */
    private boolean checkAntiPrefixes(String validPrefix) {
        for (String antiPrefix : antiPrefixes) {
            if (validPrefix.startsWith(antiPrefix)) return false;
        }
        return true;
    }

    /**
     * 从 TenLister 列表中取出对应的 TenLister 放入线程池中调用导出方法执行数据源数据导出工作，并可能进行 process 过程，记录导出结果
     * @param tenListerList 计算好的 TenLister 列表
     * @param recordFileMap 用于记录导出结果的持久化文件对象
     * @param order 目前执行的进度，已经执行多少个 FileLister 的列举
     * @throws Exception 持久化失败、列举失败等情况可能产生的异常
     */
    private void execInThreads(List<TenLister> tenListerList, FileMap recordFileMap, int order) throws Exception {
        for (int j = 0; j < tenListerList.size(); j++) {
            TenLister tenLister = tenListerList.get(j);
            // 如果是第一个线程直接使用初始的 processor 对象，否则使用 clone 的 processor 对象，多线程情况下不要直接使用传入的 processor，
            //            // 因为对其关闭会造成 clone 的对象无法进行结果持久化的写入
            ILineProcess<Map<String, String>> lineProcessor = processor == null ? null : processor.clone();
            // 持久化结果标识信息
            String identifier = String.valueOf(j + 1 + order);
            FileMap fileMap = new FileMap(savePath, "bucketlist", identifier);
            fileMap.initDefaultWriters();
            executorPool.execute(() -> {
                try {
                    String record = "order " + identifier + ": " + tenLister.getPrefix();
                    recordFileMap.writeKeyFile("result", record + "\tlisting...", true);
                    export(tenLister, fileMap, lineProcessor);
                    record += "\tsuccessfully done";
                    System.out.println(record);
                    recordFileMap.writeKeyFile("result", record, true);
                    fileMap.closeWriters();
                    if (lineProcessor != null) lineProcessor.closeResource();
                    tenLister.remove();
                } catch (Exception e) {
                    System.out.println("order " + identifier + ": " + tenLister.getPrefix() + "\tmarker: " +
                            tenLister.getMarker() + "\tend:" + tenLister.getEndKeyPrefix());
                    recordFileMap.closeWriters();
                    fileMap.closeWriters();
                    if (lineProcessor != null) lineProcessor.closeResource();
                    SystemUtils.exit(exitBool, e);
                }
            });
        }
    }

    /**
     * 最后一段 FileLister 修改前缀 prefix 和下一个 marker，该方法是为了确保实际文件名前缀在最大的预定义前缀之后的文件被成功列举
     * @param lastLister 通过计算得到的所有确定前缀下的列举器列表
     * @param prefix 计算该列举器列表时所用的前缀
     */
    private void updateLastLiter(TenLister lastLister, String prefix) {
        if (lastLister != null) {
            int size = lastLister.getCosObjectList().size();
            lastLister.setPrefix(prefix);
            if (!lastLister.checkMarkerValid()) {
                // 实际上传过来的 FileLister 在下一个 marker 为空的情况下 FileInfoList 是应该一定包含数据的
                COSObjectSummary cosObjectSummary = size > 0 ? lastLister.getCosObjectList().get(size -1) : null;
//                lastLister.setMarker(ListBucketUtils.calcMarker(cosObjectSummary));
            }
        }
    }

    private List<TenLister> generateNextList(String startPrefix, String point) {
        List<TenLister> prefixListerList = null;
        try {
            // 不要使用 parallelStream，因为上层已经使用了 parallel，再使用会导致异常崩溃：
            // java.util.concurrent.RejectedExecutionException: Thread limit exceeded replacing blocked worker
            prefixListerList = originPrefixList.stream()
                    .filter(originPrefix -> originPrefix.compareTo(point) >= 0)
                    .filter(this::checkAntiPrefixes)
                    .map(originPrefix -> {
                        TenLister tenLister = null;
                        try {
                            tenLister = generateLister(startPrefix + originPrefix);
                        } catch (IOException e) {
                            SystemUtils.exit(exitBool, e);
                        }
                        return tenLister;
                    })
                    .filter(lister -> lister != null && lister.hasNext())
                    .collect(Collectors.toList());
        } catch (Throwable e) {
            SystemUtils.exit(exitBool, e);
        }

        return prefixListerList;
    }

    /**
     * 通过 FileLister 得到目前列举出的最大文件名首字母和前缀列表进行比较，筛选出在当前列举位置之后的单字母前缀列表，该方法是为了优化前缀检索过程
     * 中算法复杂度，通过剔除在当前列举位置之前的前缀，减少不必要的检索，如果传过来的 FileLister 的 FileInfoList 中没有数据的话应当是存在下一
     * 个 marker 的（可能是文件被删除的情况），也可以直接检索下一级前缀（所有的前缀字符都比 "" 大），对初始的列举器更新参数并对所有有效的下一级前
     * 缀进行检索并生成列举器列表
     * @param tenLister 当前的 tenLister 参数，不能为空
     * @return 根据检索结果得到的下一级列举器列表
     */
    private List<TenLister> nextLevelLister(TenLister tenLister) {
        // 如果没有可继续的 marker 的话则不需要再往前进行检索了，直接返回仅包含该 fileLister 的列表
        List<TenLister> nextLevelList = new ArrayList<>();
        if (!tenLister.checkMarkerValid()) {
            nextLevelList.add(tenLister);
            return nextLevelList;
        }

        // 用于下次列举的 marker 实际上是通过此次列举到的最后一个文件信息（包括已经删除的文件）编码出来的，因此通过下一个 marker 可解析出最后一
        // 个文件信息即已经列举到的位置
        FileInfo nextFileInfo = ListBucketUtils.decodeMarker(tenLister.getMarker());
        String lastKey = nextFileInfo.key;
        // 计算出当前列举使用的前缀往前的一个字符，用于下级前缀的检索
        int prefixLen = tenLister.getPrefix().length();
        String point = "";
        if (lastKey.length() > prefixLen + 1) point = lastKey.substring(prefixLen, prefixLen + 1);
        else if (lastKey.length() == prefixLen + 1) point = lastKey.substring(prefixLen);

        // 如果此时下一个字符比预定义的最后一个前缀大的话（如中文文件名的情况）说明后续根据预定义前缀再检索无意义，则直接返回即可
        if (point.compareTo(originPrefixList.get(originPrefixList.size() - 1)) > 0) {
            tenLister.setEndKeyPrefix(null);
            nextLevelList.add(tenLister);
            return nextLevelList;
        }

        List<COSObjectSummary> cosObjectSummaries = tenLister.getCosObjectList();
        if (cosObjectSummaries != null && cosObjectSummaries.size() > 0) {
            // 如果得到的列表中第一个文件和最后一个文件的下一级前缀是相同的话，说明此次列举只有一个下级前缀，则不需要将此 fileLister
            // 添加进列表，反之则应该添加之列表中，且根据最后一个文件名下一级前缀来设置 endKeyPrefix
            if (!cosObjectSummaries.get(0).getKey().startsWith(tenLister.getPrefix() + point)) {
                nextLevelList.add(tenLister);
            } else if (point.compareTo(originPrefixList.get(0)) < 0) {
                nextLevelList.add(tenLister);
            }
        } else { // 文件存在删除的情况，fileInfoList 为空
            nextLevelList.add(tenLister);
        }

        // 避免出现字符在预定义前缀字符 ASCII 顺序之前的情况，至少保证结束位置在第一个预定义前缀处，因此前缀检索最小的从第一个预定义前缀开始
        if (point.compareTo(originPrefixList.get(0)) < 0) {
            point = originPrefixList.get(0);
        }
        // 当前的 fileLister 应该设置 endKeyPrefix 到 point 处，从 point 处开始会进行下一级检索
        tenLister.setEndKeyPrefix(tenLister.getPrefix() + point);
        List<TenLister> prefixListerList = generateNextList(tenLister.getPrefix(), point);
        if (prefixListerList != null) nextLevelList.addAll(prefixListerList);
        return nextLevelList;
    }

    /**
     * 根据 startLister（及其 prefix 参数）和有序的前缀列表往前计算下级有效的前缀并得到新的 FileLister 放入线程执行列举
     * @param lastPrefix startLister 的 prefix 条件下能够得到的 FileLister 列表中最后一个 FileLister 需要更新的前缀
     * @return 此次计算并执行的 FileLister 个数，用于后续可能继续添加 FileLister 执行，编号需要进行递增
     * @throws IOException FileLister 列表放入线程执行过程中可能产生的异常
     */
    private int computeToList(TenLister startLister, String lastPrefix, int alreadyOrder, FileMap recordFileMap)
            throws Exception {
        List<TenLister> tenListerList = nextLevelLister(startLister);
        List<TenLister> execListerList = new ArrayList<>();
        boolean lastListerUpdated = false;
        TenLister lastLister;
        int nextSize;
        // 避免重复生成新对象，将 groupedListerMap 放在循环外部
        Map<Boolean, List<TenLister>> groupedListerMap;
        do {
            // 取出前缀最大的 FileLister，如果其不存在下一个 marker，则不需要继续往下计算前缀，那么这将是整体的最后一个 FileLister，为了保
            // 证能列举到 lastPrefix 的所有文件，需要对其现有的 prefix 做更新，一般情况下 startLister.getPrefix() 是和 lastPrefix 相
            // 同的，但是如果初始参数 prefixes 不为空且 prefixRight 为 true 的情况下表示需要列举 prefixes 中最后一个前缀开始的文件之后的
            // 所有文件，则需要将此 FileLister 更新前缀为 lastPrefix
            if (!lastListerUpdated) {
                tenListerList.sort(Comparator.comparing(TenLister::getPrefix));
                lastLister = tenListerList.get(tenListerList.size() - 1);
                if (lastLister != null && !lastLister.checkMarkerValid()) {
                    updateLastLiter(lastLister, lastPrefix);
                    execListerList.add(lastLister);
                    lastListerUpdated = true;
                    // 由于 updateLastLiter 操作会更新 marker，避免下一步操作将 lastLister 再加入检索，将其独立处理，只对其他的进行分组
                    tenListerList = tenListerList.subList(0, tenListerList.size() -1 );
                }
            }
            // 给 progressiveList 按照是否有下一个 marker 进行分组，有下个 marker 的对象进一步进行前缀检索查询，没有下个 marker 的对象
            // 先添加进列表，整个列表 size 达到线程个数时即可放入线程池进行并发列举
            // 加入 "".equals(fileLister.getEndKeyPrefix()) 过滤的原因是因为经过 nextLevelLister 处理时修改了 endKeyPrefix 的需
            // 要将其直接放入线程执行不做进一步前缀检索
            groupedListerMap = tenListerList.parallelStream().collect(Collectors.groupingBy(fileLister ->
                    fileLister.checkMarkerValid() && "".equals(fileLister.getEndKeyPrefix())));
            if (groupedListerMap.get(false) != null) execListerList.addAll(groupedListerMap.get(false));
            // 将没有下一个 marker 的 FileLister 先放入线程执行掉
            execInThreads(execListerList, recordFileMap, alreadyOrder);
            alreadyOrder += execListerList.size();
            execListerList.clear();
            // 有下一个有效 marker 的 FileLister 可以用于继续检索前缀
            if (groupedListerMap.get(true) != null) {
                Optional<List<TenLister>> listOptional = groupedListerMap.get(true).parallelStream()
                        .map(this::nextLevelLister)
                        .reduce((list1, list2) -> { list1.addAll(list2); return list1; });
                if (listOptional.isPresent() && listOptional.get().size() > 0) {
                    tenListerList = listOptional.get();
                    nextSize = (int) tenListerList.parallelStream()
                            .filter(fileLister -> fileLister.checkMarkerValid() && "".equals(fileLister.getEndKeyPrefix()))
                            .count();
                } else {
                    tenListerList = groupedListerMap.get(true);
                    break;
                }
            } else {
                tenListerList.clear();
                break;
            }
        } while (nextSize < threads);

        // 如果没有更新过整体的最后一个 FileLister 则必须对计算得到的列举器列表中最大的一个 FileLister 进行更新
        if (!lastListerUpdated && tenListerList.size() > 0) {
            tenListerList.sort(Comparator.comparing(TenLister::getPrefix));
            lastLister = tenListerList.get(tenListerList.size() - 1);
            updateLastLiter(lastLister, lastPrefix);
        }
        execInThreads(tenListerList, recordFileMap, alreadyOrder);
        alreadyOrder += tenListerList.size();
        return alreadyOrder;
    }

    /**
     * 启动多线程导出的方法，根据线程数自动执行多线程并发导出
     * @throws Exception 计算 FileLister 列表失败或者写入失败等情况下的异常
     */
    public void export() throws Exception {
        String info = "list bucket: " + bucket + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " running...");
        FileMap recordFileMap = new FileMap(savePath);
        executorPool = Executors.newFixedThreadPool(threads);
        exitBool = new AtomicBoolean(false);
        Collections.sort(prefixes);
        int alreadyOrder = 0;
        if (prefixes.size() == 0) {
            TenLister startLister = generateLister("");
            computeToList(startLister, "", alreadyOrder, recordFileMap);
        } else {
            if (prefixLeft) {
                TenLister startLister = generateLister("");
                startLister.setEndKeyPrefix(prefixes.get(0));
                execInThreads(new ArrayList<TenLister>(){{ add(startLister); }}, recordFileMap, alreadyOrder);
                alreadyOrder += 1;
            }
            for (int i = 0; i < prefixes.size() - 1; i++) {
                TenLister startLister = generateLister(prefixes.get(i));
                alreadyOrder = computeToList(startLister, prefixes.get(i), alreadyOrder, recordFileMap);
            }
            TenLister startLister = generateLister(prefixes.get(prefixes.size() - 1));
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
