package com.qiniu.service.datasource;

import com.qiniu.persistence.FileMap;
import com.qiniu.service.convert.InfoMapToString;
import com.qiniu.service.fileline.JsonLineParser;
import com.qiniu.service.fileline.SplitLineParser;
import com.qiniu.service.interfaces.ILineParser;
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

    private ILineParser lineParser;
    private int unitLen;
    private int retryCount;
    private String resultFileDir;
    private boolean saveTotal = false;
    private String resultFormat;
    private String separator;

    public FileInput(String parseType, String separator, Map<String, String> infoIndexMap, int retryCount, int unitLen,
                     String resultFileDir) {
        if ("json".equals(parseType)) {
            this.lineParser = new JsonLineParser(infoIndexMap);
        } else {
            this.lineParser = new SplitLineParser(separator, infoIndexMap);
        }
        this.unitLen = unitLen;
        this.retryCount = retryCount;
        this.resultFileDir = resultFileDir;
    }

    public void setSaveTotalOptions(String resultFormat, String separator) {
        this.saveTotal = true;
        this.resultFormat = resultFormat;
        this.separator = separator;
    }

    public void traverseByReader(int finalI, BufferedReader bufferedReader, ILineProcess<Map<String, String>> processor) {

        FileMap fileMap = new FileMap();
        ILineProcess<Map<String, String>> fileProcessor = null;
        try {
            fileMap.initWriter(resultFileDir, "fileinput", finalI + 1);
            if (processor != null) fileProcessor = processor.getNewInstance(finalI + 1);
            List<Map<String, String>> infoMapList = bufferedReader.lines().parallel()
                    .filter(line -> line != null && !"".equals(line))
                    .map(line -> {
                        Map<String, String> infoMap = null;
                        try {
                            infoMap = lineParser.getItemMap(line);
                        } catch (Exception e) {
                            while (retryCount > 0) {
                                System.out.println("covert input line:" + line + "to map failed. retrying...");
                                try {
                                    retryCount = 0;
                                } catch (Exception e1) {
                                    retryCount--;
                                    if (retryCount <= 0)
                                        fileMap.writeErrorOrNull(line + "\t" + e1.getCause());
                                }
                            }
                        }
                        return infoMap;
                    })
                    .collect(Collectors.toList());
            if (saveTotal) {
                ITypeConvert<Map<String, String>, String> typeConverter = new InfoMapToString(resultFormat, separator,
                        true, true, true, true, true, true, true);
                fileMap.writeSuccess(String.join("\n", typeConverter.convertToVList(infoMapList)));
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
