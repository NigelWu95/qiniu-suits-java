package com.qiniu.datasource;

import com.qiniu.common.QiniuException;
import com.qiniu.entry.CommonParams;
import com.qiniu.persistence.FileMap;
import com.qiniu.convert.MapToString;
import com.qiniu.convert.LineToMap;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.SystemUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class FileInput implements IDataSource {

    private String filePath;
    private String parseType;
    private String separator;
    private Map<String, String> indexMap;
    private int unitLen;
    private int retryTimes = 5;
    private int threads;
    private String savePath;
    private String saveTag;
    private boolean saveTotal;
    private String saveFormat;
    private String saveSeparator;
    private List<String> rmFields;
    private ExecutorService executorPool; // 线程池
    private AtomicBoolean exitBool; // 多线程的原子操作 bool 值
    private ILineProcess<Map<String, String>> processor; // 定义的资源处理器

    public FileInput(String filePath, String parseType, String separator, Map<String, String> indexMap, int unitLen,
                     int threads, String savePath) {
        this.filePath = filePath;
        this.parseType = parseType;
        this.separator = separator;
        this.indexMap = indexMap;
        this.unitLen = unitLen;
        this.threads = threads;
        this.savePath = savePath;
        this.saveTag = "";
    }

    // 不调用则各参数使用默认值
    public void setSaveOptions(boolean saveTotal, String saveTag, String format, String separator, List<String> rmFields) {
        this.saveTotal = saveTotal;
        this.saveTag = saveTag == null ? "" : saveTag;
        this.saveFormat = format;
        this.saveSeparator = separator;
        this.rmFields = rmFields;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    // 通过 commonParams 来更新基本参数
    public void updateSettings(CommonParams commonParams) {
        this.filePath = commonParams.getPath();
        this.parseType = commonParams.getParse();
        this.separator = commonParams.getSeparator();
        this.indexMap = commonParams.getIndexMap();
        this.unitLen = commonParams.getUnitLen();
        this.threads = commonParams.getThreads();
        this.savePath = commonParams.getSavePath();
        this.saveTag = commonParams.getSaveTag();
        this.saveTotal = commonParams.getSaveTotal();
        this.saveFormat = commonParams.getSaveFormat();
        this.saveSeparator = commonParams.getSaveSeparator();
        this.rmFields = commonParams.getRmFields();
    }

    public void setProcessor(ILineProcess<Map<String, String>> processor) {
        this.processor = processor;
    }

    private void export(BufferedReader reader, FileMap fileMap, ILineProcess<Map<String, String>> processor)
            throws IOException {
        ITypeConvert<String, Map<String, String>> typeConverter = new LineToMap(parseType, separator, indexMap);
        ITypeConvert<Map<String, String>, String> writeTypeConverter = new MapToString(saveFormat, saveSeparator, rmFields);
        List<String> srcList = new ArrayList<>();
        List<Map<String, String>> infoMapList;
        List<String> writeList;
        String line = "";
        int retry;
        while (line != null) {
            retry = retryTimes + 1;
            while (retry > 0) {
                try {
                    // 避免文件过大，行数过多，使用 lines() 的 stream 方式直接转换可能会导致内存泄漏，故使用 readLine() 的方式
                    line = reader.readLine();
                    retry = 0;
                } catch (IOException e) {
                    retry--;
                    if (retry == 0) throw e;
                }
            }
            if (line != null) srcList.add(line);
            if (srcList.size() >= unitLen || (line == null && srcList.size() > 0)) {
                infoMapList = typeConverter.convertToVList(srcList);
                if (typeConverter.getErrorList().size() > 0)
                    fileMap.writeError(String.join("\n", typeConverter.consumeErrorList()), false);
                if (saveTotal) {
                    writeList = writeTypeConverter.convertToVList(infoMapList);
                    if (writeList.size() > 0) fileMap.writeSuccess(String.join("\n", writeList), false);
                    if (writeTypeConverter.getErrorList().size() > 0)
                        fileMap.writeError(String.join("\n", writeTypeConverter.consumeErrorList()), false);
                }
                // 如果抛出异常需要检测下异常是否是可继续的异常，如果是程序可继续的异常，忽略当前异常保持数据源读取过程继续进行
                try {
                    if (processor != null) processor.processLine(infoMapList);
                } catch (QiniuException e) {
                    // 这里其实逻辑上没有做重试次数的限制，因此 retry > 0，所以不是必须抛出的异常则会跳过，process 本身会保存失败的记录
                    retry = HttpResponseUtils.checkException(e, retry);
                    if (retry == -1) throw e;
                }
                srcList.clear();
            }
        }
    }

    private void execInThreads(FileMap initFileMap) throws Exception {
        HashMap<String, BufferedReader> readersMap = initFileMap.getReaderMap();
        List<String> keys = new ArrayList<>(readersMap.keySet());
        for (int i = 0; i < keys.size(); i++) {
            // 如果是第一个线程直接使用初始的 processor 对象，否则使用 clone 的 processor 对象，多线程情况下不要直接使用传入的 processor，
            // 因为对其关闭会造成 clone 的对象无法进行结果持久化的写入
            ILineProcess<Map<String, String>> lineProcessor = processor == null ? null : processor.clone();
            String order = String.valueOf(i);
            String key = keys.get(i);
            BufferedReader reader = readersMap.get(key);
            FileMap fileMap = new FileMap(savePath, "fileinput" + saveTag, order);
            fileMap.initDefaultWriters();
            executorPool.execute(() -> {
                try {
                    String record = "order " + order + ": " + key;
                    initFileMap.writeKeyFile("input" + saveTag + "_result", record + "\treading...", true);
                    export(reader, fileMap, lineProcessor);
                    record += "\tsuccessfully done";
                    System.out.println(record);
                    initFileMap.writeKeyFile("input" + saveTag + "_result", record, true);
                    fileMap.closeWriters();
                    if (lineProcessor != null) lineProcessor.closeResource();
                } catch (Exception e) {
                    try {
                        System.out.println("order " + order + ": " + key + "\tnextLine:" + reader.readLine());
                    } catch (IOException io) {
                        io.printStackTrace();
                    }
                    initFileMap.closeWriters();
                    fileMap.closeWriters();
                    if (lineProcessor != null) lineProcessor.closeResource();
                    SystemUtils.exit(exitBool, e);
                }
            });
        }
    }

    public void export() throws Exception {
        FileMap initFileMap = new FileMap(savePath);
        File sourceFile = new File(filePath);
        if (sourceFile.isDirectory()) {
            initFileMap.initReaders(filePath);
        } else {
            initFileMap.initReader(filePath);
        }

        int filesCount = initFileMap.getReaderMap().size();
        int runningThreads = filesCount < threads ? filesCount : threads;
        String info = "read files: " + filePath + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " running...");
        executorPool = Executors.newFixedThreadPool(runningThreads);
        exitBool = new AtomicBoolean(false);
        execInThreads(initFileMap);
        executorPool.shutdown();
        while (!executorPool.isTerminated()) Thread.sleep(1000);
        initFileMap.closeReaders();
        System.out.println(info + " finished");
    }
}
