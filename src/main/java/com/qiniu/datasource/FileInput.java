package com.qiniu.datasource;

import com.qiniu.common.QiniuException;
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

    final private String filePath;
    final private String parseType;
    final private String separator;
    final private Map<String, String> indexMap;
    final private int unitLen;
    final private int threads;
    final private String savePath;
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
            retry = 5;
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
                    retry = HttpResponseUtils.checkException(e, 1);
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
            FileMap fileMap = new FileMap(savePath, "fileinput", order);
            fileMap.initDefaultWriters();
            executorPool.execute(() -> {
                try {
                    String record = "order " + order + ": " + key;
                    initFileMap.writeKeyFile("result", record + "\treading...", true);
                    export(reader, fileMap, lineProcessor);
                    record += "\tsuccessfully done";
                    System.out.println(record);
                    initFileMap.writeKeyFile("result", record, true);
                    fileMap.closeWriters();
                    if (lineProcessor != null) lineProcessor.closeResource();
                } catch (Exception e) {
                    try {
                        System.out.println("order " + order + ": " + key + "\tnextLine:" + reader.readLine());
                    } catch (IOException ioE) {
                        ioE.printStackTrace();
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
        String info = "read files" + (processor == null ? "" : " and " + processor.getProcessName());
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
