package com.qiniu.service.datasource;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.service.fileline.SplitLineParser;
import com.qiniu.service.interfaces.ILineParser;
import com.qiniu.service.interfaces.IOssFileProcess;
import com.qiniu.service.media.QueryAvinfo;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.ExecutorsUtils;
import com.qiniu.util.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

public class FileInput {

    private String separator;
    private int keyIndex;
    private int unitLen;
    private int retryCount;

    public FileInput(String separator, int keyIndex, int unitLen, int retryCount) {
        this.separator = separator;
        this.keyIndex = keyIndex;
        this.unitLen = unitLen;
        this.retryCount = retryCount;
    }

//    public void traverseByReader(int finalI, List<BufferedReader> sourceReaders, QueryAvinfo fileProcessor) {
//
//        QueryAvinfo processor = null;
//        try {
//            BufferedReader bufferedReader = sourceReaders.get(finalI);
//            if (fileProcessor != null) processor = (QueryAvinfo) fileProcessor.getNewInstance(finalI + 1);
//            List<String> fileInfoList = bufferedReader.lines().parallel().collect(Collectors.toList());
//            int size = fileInfoList.size()/unitLen + 1;
//            for (int j = 0; j < size; j++) {
//                List<String > processList = fileInfoList.subList(1000 * j,
//                        j == size - 1 ? fileInfoList.size() : 1000 * (j + 1));
//                if (processor != null) processor.processFile(processList);
//            }
//            bufferedReader.close();
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        } finally {
//            if (processor != null) processor.closeResource();
//        }
//    }

    public void traverseByReader(int finalI, List<BufferedReader> sourceReaders, IOssFileProcess fileProcessor) {

        IOssFileProcess processor = null;
        ILineParser lineParser = new SplitLineParser(separator);
        try {
            BufferedReader bufferedReader = sourceReaders.get(finalI);
            if (fileProcessor != null) processor = fileProcessor.getNewInstance(finalI + 1);
            List<FileInfo> fileInfoList = bufferedReader.lines().parallel()
                    .map(line -> {
                        FileInfo fileInfo = new FileInfo();
                        fileInfo.key = lineParser.getItemList(line).get(keyIndex);
                        return fileInfo;
                    })
                    .filter(fileInfo -> !StringUtils.isNullOrEmpty(fileInfo.key))
                    .collect(Collectors.toList());
            int size = fileInfoList.size()/unitLen + 1;
            for (int j = 0; j < size; j++) {
                List<FileInfo> processList = fileInfoList.subList(1000 * j,
                        j == size - 1 ? fileInfoList.size() : 1000 * (j + 1));
                if (processor != null) processor.processFile(processList, retryCount);
            }
            bufferedReader.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (processor != null) processor.closeResource();
        }
    }

    public void process(int maxThreads, String filePath, IOssFileProcess processor) {
        List<String> sourceKeys = new ArrayList<>();
        FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap();
        File sourceFile = new File(filePath);
        try {
            if (sourceFile.isDirectory()) {
                File[] fs = sourceFile.listFiles();
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
//            executorPool.execute(() -> traverseByReader(finalI, sourceReaders, (QueryAvinfo) processor));
        }
        executorPool.shutdown();
        ExecutorsUtils.waitForShutdown(executorPool, info);
        fileMap.closeWriter();
    }
}