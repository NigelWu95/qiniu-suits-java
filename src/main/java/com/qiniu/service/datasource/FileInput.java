package com.qiniu.service.datasource;

import com.qiniu.common.FileMap;
import com.qiniu.service.fileline.SplitLineParser;
import com.qiniu.service.interfaces.ILineParser;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.ExecutorsUtils;
import com.qiniu.util.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

public class FileInput {

    private String separator;
    private int keyIndex;
    private int unitLen;

    public FileInput(String separator, int keyIndex, int unitLen) {
        this.separator = separator;
        this.keyIndex = keyIndex;
        this.unitLen = unitLen;
    }

    public void traverseByReader(int finalI, List<BufferedReader> sourceReaders, ILineProcess fileProcessor) {

        ILineProcess processor = null;
        ILineParser lineParser = new SplitLineParser(separator);
        try {
            BufferedReader bufferedReader = sourceReaders.get(finalI);
            if (fileProcessor != null) processor = fileProcessor.getNewInstance(finalI + 1);
            List<Map<String, String>> fileInfoList = bufferedReader.lines().parallel()
                    .map(lineParser::getItemMap)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            int size = fileInfoList.size()/unitLen + 1;
            for (int j = 0; j < size; j++) {
                List<Map<String, String>> processList = fileInfoList.subList(unitLen * j,
                        j == size - 1 ? fileInfoList.size() : unitLen * (j + 1));
                if (processor != null) processor.processLine(processList);
            }
            bufferedReader.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (processor != null) processor.closeResource();
        }
    }

    public void process(int maxThreads, String filePath, ILineProcess processor) {
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
            executorPool.execute(() -> traverseByReader(finalI, sourceReaders, processor));
        }
        executorPool.shutdown();
        ExecutorsUtils.waitForShutdown(executorPool, info);
        fileMap.closeWriter();
    }
}
