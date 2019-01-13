package com.qiniu.service.datasource;

import com.qiniu.persistence.FileMap;
import com.qiniu.service.convert.InfoMapToString;
import com.qiniu.service.convert.LineToInfoMap;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.util.ExecutorsUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class FileInput implements IDataSource {

    final private String filePath;
    final private String parseType;
    final private String separator;
    final private Map<String, String> infoIndexMap;
    final private int unitLen;
    final private String resultPath;
    private boolean saveTotal;
    private String resultFormat;
    private String resultSeparator;
    private List<String> removeFields;

    public FileInput(String filePath, String parseType, String separator, Map<String, String> infoIndexMap, int unitLen,
                     String resultPath) {
        this.filePath = filePath;
        this.parseType = parseType;
        this.separator = separator;
        this.infoIndexMap = infoIndexMap;
        this.unitLen = unitLen;
        this.resultPath = resultPath;
        this.saveTotal = false;
    }

    public void setResultSaveOptions(String format, String separator, List<String> removeFields) {
        this.saveTotal = true;
        this.resultFormat = format;
        this.resultSeparator = separator;
        this.removeFields = removeFields;
    }

    private void traverseByReader(BufferedReader reader, FileMap fileMap, ILineProcess processor) throws Exception {
        ITypeConvert<String, Map<String, String>> typeConverter = new LineToInfoMap(parseType, separator, infoIndexMap);
        ITypeConvert<Map<String, String>, String> writeTypeConverter = null;
        if (saveTotal) {
            writeTypeConverter = new InfoMapToString(resultFormat, resultSeparator, removeFields);
            fileMap.initDefaultWriters();
        }
        List<String> srcList = new ArrayList<>();
        String line;
        boolean goon = true;
        while (goon) {
            // 避免文件过大，行数过多，使用 lines() 的 stream 方式直接转换可能会导致内存泄漏，故使用 readLine() 的方式
            line = reader.readLine();
            if (line == null) goon = false;
            else srcList.add(line);
            if (srcList.size() >= unitLen || line == null) {
                List<Map<String, String>> infoMapList = typeConverter.convertToVList(srcList);
                List<String> writeList;
                if (typeConverter.getErrorList().size() > 0)
                    fileMap.writeKeyFile("map_error", String.join("\n", typeConverter.consumeErrorList()));
                if (saveTotal) {
                    writeList = writeTypeConverter.convertToVList(infoMapList);
                    if (writeList.size() > 0) fileMap.writeSuccess(String.join("\n", writeList));
                    if (writeTypeConverter.getErrorList().size() > 0)
                        fileMap.writeError(String.join("\n", writeTypeConverter.consumeErrorList()));
                }
                int size = infoMapList.size() / unitLen + 1;
                for (int j = 0; j < size; j++) {
                    List<Map<String, String>> processList = infoMapList.subList(unitLen * j,
                            j == size - 1 ? infoMapList.size() : unitLen * (j + 1));
                    if (processor != null) processor.processLine(processList);
                }
                srcList = new ArrayList<>();
            }
        }
    }

    private FileMap getSourceFileMap() {
        FileMap inputFileMap = new FileMap();
        File sourceFile = new File(filePath);
        try {
            if (sourceFile.isDirectory()) {
                inputFileMap.initReaders(filePath);
            } else {
                inputFileMap.initReader(sourceFile.getParent(), sourceFile.getName());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return inputFileMap;
    }

    public void exportData(int threads, ILineProcess<Map<String, String>> processor) throws Exception {
        FileMap inputFileMap = getSourceFileMap();
        Set<Entry<String, BufferedReader>> readerEntrySet = inputFileMap.getReaderMap().entrySet();
        int listSize = readerEntrySet.size();
        int runningThreads = listSize < threads ? listSize : threads;
        String info = "read files" + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " concurrently running with " + runningThreads + " threads ...");
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable);
            thread.setUncaughtExceptionHandler((t, e) -> {
                System.out.println(t.getName() + "\t" + t.toString());
                e.printStackTrace();
            });
            return thread;
        };
        ExecutorService executorPool = Executors.newFixedThreadPool(runningThreads, threadFactory);
        List<String> linePositionList = new ArrayList<>();
        for (Entry<String, BufferedReader> readerEntry : readerEntrySet) {
            if (processor != null) processor.setResultTag(readerEntry.getKey());
            ILineProcess lineProcessor = processor == null ? null : processor.clone();
            FileMap fileMap = new FileMap(resultPath, "fileinput", readerEntry.getKey());
            Thread thread = new Thread(() -> {
                String exception = "";
                try {
                    traverseByReader(readerEntry.getValue(), fileMap, lineProcessor);
                } catch (Exception e) {
                    exception = "\t" + e.getMessage();
                    throw new RuntimeException(e);
                } finally {
                    String nextLine;
                    try {
                        nextLine = readerEntry.getValue().readLine();
                    } catch (IOException e) {
                        nextLine = e.getMessage();
                    }
                    nextLine += exception;
                    String record = "order " + fileMap.getSuffix() + ": " + readerEntry.getKey();
                    if ("".equals(nextLine) || "null".equals(nextLine)) {
                        linePositionList.add(record + "\tsuccessfully done");
                        System.out.println(record + "\tsuccessfully done");
                    } else {
                        linePositionList.add(record + "\t" + nextLine);
                        System.out.println(record + "\t" + nextLine);
                    }
                    fileMap.closeWriters();
                    if (lineProcessor != null) lineProcessor.closeResource();
                    inputFileMap.closeReader(readerEntry.getKey());
                }
            });
            thread.setName(readerEntry.getKey());
            executorPool.execute(thread);
        }
        executorPool.shutdown();
        ExecutorsUtils.waitForShutdown(executorPool, info);
        FileMap fileMap = new FileMap(resultPath);
        fileMap.writeKeyFile("result" + new Date().getTime(), String.join("\n", linePositionList));
        fileMap.closeWriters();
    }
}
