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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

public class FileInput {

    private String parseType;
    private String separator;
    private Map<String, String> infoIndexMap;
    private int unitLen;
    private String resultPath;
    private boolean saveTotal;
    private String resultFormat;
    private String resultSeparator;
    private List<String> resultFields;

    public FileInput(String parseType, String separator, Map<String, String> infoIndexMap, int unitLen,
                     String resultPath) {
        this.parseType = parseType;
        this.separator = separator;
        this.infoIndexMap = infoIndexMap;
        this.unitLen = unitLen;
        this.resultPath = resultPath;
        this.saveTotal = false;
    }

    public void setResultSaveOptions(String format, String separator, List<String> fields) {
        this.saveTotal = true;
        this.resultFormat = format;
        this.resultSeparator = separator;
        this.resultFields = fields;
    }

    public void traverseByReader(int resultIndex, BufferedReader bufferedReader, ILineProcess<Map<String, String>> processor) {
        FileMap fileMap = new FileMap();
        ILineProcess<Map<String, String>> fileProcessor = null;
        try {
            fileMap.initWriter(resultPath, "fileinput", resultIndex);
            if (processor != null) fileProcessor = resultIndex == 0 ? processor : processor.clone();
            ITypeConvert<String, Map<String, String>> typeConverter = new LineToInfoMap(parseType, separator, infoIndexMap);
            List<String> srcList = new ArrayList<>();
            String line;
            boolean goon = true;
            while (goon) {
                // 避免文件过大，行数过多，使用 lines() 的 stream 方式直接转换可能会导致内存泄漏，故使用 readLine() 的方式
                line = bufferedReader.readLine();
                if (line == null) goon = false;
                else srcList.add(line);
                if (srcList.size() >= unitLen || line == null) {
                    List<Map<String, String>> infoMapList = typeConverter.convertToVList(srcList);
                    List<String> writeList;
                    if (typeConverter.getErrorList().size() > 0) fileMap.writeError(String.join("\n",
                            typeConverter.getErrorList()));
                    if (saveTotal) {
                        ITypeConvert<Map<String, String>, String> writeTypeConverter = new InfoMapToString(resultFormat,
                                resultSeparator, resultFields);
                        writeList = writeTypeConverter.convertToVList(infoMapList);
                        if (writeList.size() > 0) fileMap.writeSuccess(String.join("\n", writeList));
                        if (writeTypeConverter.getErrorList().size() > 0)
                            fileMap.writeError(String.join("\n", writeTypeConverter.getErrorList()));
                    }
                    int size = infoMapList.size() / unitLen + 1;
                    for (int j = 0; j < size; j++) {
                        List<Map<String, String>> processList = infoMapList.subList(unitLen * j,
                                j == size - 1 ? infoMapList.size() : unitLen * (j + 1));
                        if (fileProcessor != null) fileProcessor.processLine(processList);
                    }
                    srcList = new ArrayList<>();
                }
            }
            bufferedReader.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (fileProcessor != null) fileProcessor.closeResource();
        }
    }

    public void process(int maxThreads, String filePath, ILineProcess<Map<String, String>> processor) {
        List<String> sourceKeys = new ArrayList<>();
        FileMap fileMap = new FileMap();
        File sourceFile = new File(filePath);
        try {
            if (sourceFile.isDirectory()) {
                File[] fs = sourceFile.listFiles();
                assert fs != null;
                for(File f : fs) {
                    if (!f.isDirectory()) {
                        sourceKeys.add(f.getName());
                        fileMap.initReader(sourceFile.getPath(), f.getName());
                    }
                }
            } else {
                sourceKeys.add(sourceFile.getName());
                fileMap.initReader(sourceFile.getParent(), sourceFile.getName());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        int listSize = sourceKeys.size();
        int runningThreads = listSize < maxThreads ? listSize : maxThreads;
        String info = "read files" + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " concurrently running with " + runningThreads + " threads ...");
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable);
            thread.setUncaughtExceptionHandler((t, e) -> System.out.println(t.getName() + "\t" + e.getMessage()));
            return thread;
        };
        ExecutorService executorPool = Executors.newFixedThreadPool(runningThreads, threadFactory);
        List<BufferedReader> sourceReaders = sourceKeys.parallelStream()
                .map(fileMap::getReader)
                .collect(Collectors.toList());
        for (int i = 0; i < sourceReaders.size(); i++) {
            int finalI = i;
            executorPool.execute(() -> traverseByReader(finalI, sourceReaders.get(finalI), processor));
        }
        executorPool.shutdown();
        ExecutorsUtils.waitForShutdown(executorPool, info);
        fileMap.closeReader();
        fileMap.closeWriter();
    }
}
