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
    private String resultFileDir;
    private boolean saveTotal;
    private String resultFormat;

    public FileInput(String parseType, String separator, Map<String, String> infoIndexMap, int unitLen,
                     String resultFileDir) {
        this.parseType = parseType;
        this.separator = separator;
        this.infoIndexMap = infoIndexMap;
        this.unitLen = unitLen;
        this.resultFileDir = resultFileDir;
    }

    public void setSaveTotalOptions(boolean saveTotal, String resultFormat, String separator) {
        this.saveTotal = saveTotal;
        this.resultFormat = resultFormat;
        this.separator = separator;
    }

    public void traverseByReader(int finalI, BufferedReader bufferedReader, List<String> usedFields,
                                 ILineProcess<Map<String, String>> processor) {
        FileMap fileMap = new FileMap();
        ILineProcess<Map<String, String>> fileProcessor = null;
        try {
            fileMap.initWriter(resultFileDir, "fileinput", finalI + 1);
            if (processor != null) fileProcessor = processor.getNewInstance(finalI + 1);
            // TODO
            ITypeConvert<String, Map<String, String>> typeConverter = new LineToInfoMap(parseType, separator, infoIndexMap);
            List<String> srcList = bufferedReader.lines().parallel().collect(Collectors.toList());
            List<Map<String, String>> infoMapList = typeConverter.convertToVList(srcList);
            fileMap.writeErrorOrNull(String.join("\n", typeConverter.getErrorList()));
            if (saveTotal) {
                ITypeConvert<Map<String, String>, String> writeTypeConverter = new InfoMapToString(resultFormat, separator,
                        usedFields);
                fileMap.writeSuccess(String.join("\n", writeTypeConverter.convertToVList(infoMapList)));
                fileMap.writeKeyFile("write_error", String.join("\n", writeTypeConverter.getErrorList()));
            }
            int size = infoMapList.size()/unitLen + 1;
            for (int j = 0; j < size; j++) {
                List<Map<String, String>> processList = infoMapList.subList(unitLen * j,
                        j == size - 1 ? infoMapList.size() : unitLen * (j + 1));
                if (fileProcessor != null) fileProcessor.processLine(processList);
            }
            bufferedReader.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (fileProcessor != null) fileProcessor.closeResource();
        }
    }

    public void process(int maxThreads, String filePath, List<String> usedFields, ILineProcess<Map<String, String>> processor) {
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
            executorPool.execute(() -> traverseByReader(finalI, sourceReaders.get(finalI), usedFields, processor));
        }
        executorPool.shutdown();
        ExecutorsUtils.waitForShutdown(executorPool, info);
        fileMap.closeReader();
        fileMap.closeWriter();
    }
}
