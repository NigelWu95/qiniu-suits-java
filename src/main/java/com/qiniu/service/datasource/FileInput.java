package com.qiniu.service.datasource;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.service.fileline.SplitLineParser;
import com.qiniu.service.interfaces.ILineParser;
import com.qiniu.service.interfaces.IOssFileProcess;
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

public class FileInput {

    private String separator;
    private int keyIndex;
    private int retryCount;

    public FileInput(String separator, int keyIndex, int retryCount) {
        this.separator = separator;
        this.keyIndex = keyIndex;
        this.retryCount = retryCount;
    }

    public void traverseFile(ExecutorService executorPool, FileReaderAndWriterMap fileMap, List<String> sourceReaders,
                             IOssFileProcess iOssFileProcessor) throws CloneNotSupportedException {
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
                    iOssFileProcessor.processFile(fileInfoList, retryCount);
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
    }

    public void process(int maxThreads, String filePath, IOssFileProcess iOssFileProcessor, boolean processBatch)
            throws IOException, CloneNotSupportedException {

        List<String> sourceReaders = new ArrayList<>();
        FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap();
        File sourceFile = new File(filePath);
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
        System.out.println(iOssFileProcessor.getProcessName() + " started...");
        traverseFile(executorPool, fileMap, sourceReaders, iOssFileProcessor);
        executorPool.shutdown();
        try {
            while (!executorPool.isTerminated())
                Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(iOssFileProcessor.getProcessName() + " finished.");
    }
}