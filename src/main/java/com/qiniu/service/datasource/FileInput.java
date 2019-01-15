package com.qiniu.service.datasource;

import com.qiniu.common.QiniuException;
import com.qiniu.persistence.FileMap;
import com.qiniu.service.convert.InfoMapToString;
import com.qiniu.service.convert.LineToInfoMap;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.util.ExecutorsUtils;
import com.qiniu.util.HttpResponseUtils;

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
    private List<String> rmFields;

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
        this.rmFields = removeFields;
    }

    private void traverseByReader(BufferedReader reader, FileMap fileMap, ILineProcess processor)
            throws QiniuException {
        ITypeConvert<String, Map<String, String>> typeConverter = new LineToInfoMap(parseType, separator, infoIndexMap);
        ITypeConvert<Map<String, String>, String> writeTypeConverter = new InfoMapToString(resultFormat,
                resultSeparator, rmFields);
        List<String> srcList = new ArrayList<>();
        String line = null;
        boolean goon = true;
        while (goon) {
            // 避免文件过大，行数过多，使用 lines() 的 stream 方式直接转换可能会导致内存泄漏，故使用 readLine() 的方式
            try { line = reader.readLine(); } catch (IOException e) { e.printStackTrace(); }
            if (line == null) goon = false;
            else srcList.add(line);
            if (srcList.size() >= unitLen || line == null) {
                List<Map<String, String>> infoMapList = typeConverter.convertToVList(srcList);
                List<String> writeList;
                if (typeConverter.getErrorList().size() > 0)
                    fileMap.writeError(String.join("\n", typeConverter.consumeErrorList()));
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
                    // 如果抛出异常需要检测下异常是否是可继续的异常，如果是程序可继续的异常，忽略当前异常保持数据源读取过程继续进行
                    try {
                        if (processor != null) processor.processLine(processList);
                    } catch (QiniuException e) {
                        HttpResponseUtils.checkRetryCount(e, 1);
                    }
                }
                srcList = new ArrayList<>();
            }
        }
    }

    private FileMap getSourceFileMap() throws IOException {
        FileMap inputFileMap = new FileMap();
        File sourceFile = new File(filePath);
        if (sourceFile.isDirectory()) {
            inputFileMap.initReaders(filePath);
        } else {
            inputFileMap.initReader(sourceFile.getParent(), sourceFile.getName());
        }
        return inputFileMap;
    }

    public void export(Entry<String, BufferedReader> readerEntry, ILineProcess<Map<String, String>> processor)
            throws Exception {
        FileMap recordFileMap = new FileMap(resultPath);
        FileMap fileMap = new FileMap(resultPath, "fileinput", readerEntry.getKey());
        fileMap.initDefaultWriters();
        if (processor != null) processor.setResultTag(readerEntry.getKey());
        ILineProcess lineProcessor = processor == null ? null : processor.clone();
        String record = "order: " + readerEntry.getKey();
        String exception = "";
        try {
            traverseByReader(readerEntry.getValue(), fileMap, lineProcessor);
        } catch (IOException e) {
            exception = e.getMessage();
            e.printStackTrace();
            throw e;
        } finally {
            try {
                String nextLine = readerEntry.getValue().readLine();
                if (nextLine == null || "".equals(nextLine)) record += "\tsuccessfully done";
                else record += "\tnextLine:" + nextLine + "\t" + exception;
                System.out.println(record);
                recordFileMap.writeKeyFile("result", record.replaceAll("\n", "\t"));
            } catch (IOException e) {
                e.printStackTrace();
            }
            fileMap.closeWriters();
            recordFileMap.closeWriters();
            if (lineProcessor != null) lineProcessor.closeResource();
        }
    }

    public void export(int threads, ILineProcess<Map<String, String>> processor) throws Exception {
        FileMap inputFileMap = getSourceFileMap();
        Set<Entry<String, BufferedReader>> readerEntrySet = inputFileMap.getReaderMap().entrySet();
        int listSize = readerEntrySet.size();
        int runningThreads = listSize < threads ? listSize : threads;
        String info = "read files" + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " concurrently running with " + runningThreads + " threads ...");
        ExecutorService executorPool = Executors.newFixedThreadPool(runningThreads);
        for (Entry<String, BufferedReader> readerEntry : readerEntrySet) {
            executorPool.execute(() -> {
                try {
                    export(readerEntry, processor);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        executorPool.shutdown();
        ExecutorsUtils.waitForShutdown(executorPool, info);
    }
}
