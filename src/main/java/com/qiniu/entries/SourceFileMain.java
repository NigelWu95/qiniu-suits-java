package com.qiniu.entries;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.ILineParser;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.model.*;
import com.qiniu.service.fileline.SplitLineParser;
import com.qiniu.util.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class SourceFileMain {

    public static void runMain(boolean paramFromConfig, String[] args, String configFilePath) throws Exception {

        SourceFileParams sourceFileParams = paramFromConfig ? new SourceFileParams(configFilePath) : new SourceFileParams(args);
        String separator = sourceFileParams.getSeparator();
        String filePath = sourceFileParams.getFilePath();
        String process = sourceFileParams.getProcess();
        boolean processBatch = sourceFileParams.getProcessBatch();
        int maxThreads = sourceFileParams.getMaxThreads();
        IOssFileProcess iOssFileProcessor = ProcessChoice.getFileProcessor(paramFromConfig, args, configFilePath);
        String sourceFileDir = System.getProperty("user.dir");
        List<String> sourceReaders = new ArrayList<>();

        if (filePath.endsWith(System.getProperty("file.separator"))) {
            File sourceDir = new File(System.getProperty("user.dir") + filePath);
            File[] fs = sourceDir.listFiles();

            for(File f : fs) {
                if (!f.isDirectory()) {
                    sourceReaders.add(f.getName());
                }
            }
        } else {
            File sourceFile = new File(System.getProperty("user.dir") + filePath);
            sourceFileDir = sourceFile.getParent();
            sourceReaders.add(sourceFile.getName());
        }

        int runningThreads = sourceReaders.size();
        runningThreads = runningThreads < maxThreads ? runningThreads : maxThreads;
        System.out.println("list bucket concurrently running with " + runningThreads + " threads ...");
        ExecutorService executorPool =  Executors.newFixedThreadPool(runningThreads);
        FileReaderAndWriterMap resultFileMap = new FileReaderAndWriterMap();
        resultFileMap.initReader(sourceFileDir);
        System.out.println(process + " started...");
        ILineParser lineParser = new SplitLineParser(separator);

        for (int i = 0; i < sourceReaders.size(); i++) {
            String sourceReaderKey = sourceReaders.get(i);
            IOssFileProcess processor = iOssFileProcessor != null ? iOssFileProcessor.getNewInstance(i) : null;
            if (processor == null) break;
            executorPool.execute(() -> {
                BufferedReader bufferedReader = resultFileMap.getReader(sourceReaderKey);
                if (processBatch) {
                    List<String> fileKeyList = bufferedReader.lines().parallel()
                            .map(line -> lineParser.getItemList(line).get(0))
                            .filter(key -> !StringUtils.isNullOrEmpty(key))
                            .collect(Collectors.toList());
                    iOssFileProcessor.processFile(fileKeyList, 3);
                } else {
                    bufferedReader.lines().parallel()
                            .filter(key -> !StringUtils.isNullOrEmpty(key))
                            .forEach(line -> processor.processFile(lineParser.getItemList(line).get(0), 3));
                }

                if (iOssFileProcessor.qiniuException() != null && iOssFileProcessor.qiniuException().code() > 400) {
                    QiniuException e = iOssFileProcessor.qiniuException();
                    e.printStackTrace();
                    resultFileMap.writeErrorOrNull(sourceReaderKey + "\tprocess failed\t" + e.error());
                    e.response.close();
                }

                try {
                    bufferedReader.close();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            });
        }

        executorPool.shutdown();
        try {
            while (!executorPool.isTerminated())
                Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(process + " finished.");
        if (iOssFileProcessor != null) iOssFileProcessor.closeResource();
    }
}