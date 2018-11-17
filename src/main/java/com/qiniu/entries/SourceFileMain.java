package com.qiniu.entries;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.ILineParser;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.model.*;
import com.qiniu.service.fileline.SplitLineParser;
import com.qiniu.storage.model.FileInfo;
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
        String filePath = sourceFileParams.getFilePath();
        String separator = sourceFileParams.getSeparator();
        int keyIndex = sourceFileParams.getKeyIndex();
        String process = sourceFileParams.getProcess();
        boolean processBatch = sourceFileParams.getProcessBatch();
        int maxThreads = sourceFileParams.getMaxThreads();
        String resultFileDir = sourceFileParams.getResultFileDir();
        IOssFileProcess iOssFileProcessor = ProcessChoice.getFileProcessor(paramFromConfig, args, configFilePath);
        List<String> sourceReaders = new ArrayList<>();
        FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap();
        String sourceFilePath = System.getProperty("user.dir") + System.getProperty("file.separator") + filePath;
        File sourceFile = new File(sourceFilePath);
        if (sourceFile.isDirectory()) {
            File[] fs = sourceFile.listFiles();
            for(File f : fs) {
                if (!f.isDirectory()) {
                    sourceReaders.add(f.getName());
                    fileMap.initReader(sourceFile.getPath(), f.getName());
                }
            }
        } else {
            sourceReaders.add(sourceFile.getName());
            fileMap.initReader(sourceFile.getParent(), sourceFile.getName());
        }

        int runningThreads = sourceReaders.size();
        runningThreads = runningThreads < maxThreads ? runningThreads : maxThreads;
        System.out.println("list bucket concurrently running with " + runningThreads + " threads ...");
        ExecutorService executorPool =  Executors.newFixedThreadPool(runningThreads);
        System.out.println(process + " started...");

        for (int i = 0; i < sourceReaders.size(); i++) {
            String sourceReaderKey = sourceReaders.get(i);
            IOssFileProcess processor = iOssFileProcessor != null ? iOssFileProcessor.getNewInstance(i) : null;
            if (processor == null) break;
            executorPool.execute(() -> {
                BufferedReader bufferedReader = fileMap.getReader(sourceReaderKey);
                ILineParser lineParser = new SplitLineParser(separator);

                try {
                    List<FileInfo> fileInfoList = bufferedReader.lines().parallel()
                            .map(line -> {
                                FileInfo fileInfo = new FileInfo();
                                fileInfo.key = lineParser.getItemList(line).get(keyIndex);
                                return fileInfo;
                            })
                            .filter(fileInfo -> !StringUtils.isNullOrEmpty(fileInfo.key))
                            .collect(Collectors.toList());
                    iOssFileProcessor.processFile(fileInfoList, processBatch, 3);
                } catch (QiniuException e) {
                    e.printStackTrace();
                    System.out.println(sourceReaderKey + "\tprocess failed\t" + e.error());
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
