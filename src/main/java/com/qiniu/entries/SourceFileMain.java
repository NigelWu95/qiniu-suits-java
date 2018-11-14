package com.qiniu.entries;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.Zone;
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
import java.util.stream.Collectors;

public class SourceFileMain {

    public static void runMain(boolean paramFromConfig, String[] args, String configFilePath) throws Exception {

        SourceFileParams sourceFileParams = paramFromConfig ? new SourceFileParams(configFilePath) : new SourceFileParams(args);
        String separator = sourceFileParams.getSeparator();
        String filePath = sourceFileParams.getFilePath();
        String process = sourceFileParams.getProcess();
        boolean processBatch = sourceFileParams.getProcessBatch();
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

        try {
            FileReaderAndWriterMap targetFileReaderAndWriterMap = new FileReaderAndWriterMap();
            targetFileReaderAndWriterMap.initReader(sourceFileDir);
            System.out.println(process + " started...");
            ILineParser lineParser = new SplitLineParser(separator);;

            for (int i = 0; i < sourceReaders.size(); i++) {
                String sourceReaderKey = sourceReaders.get(i);
                IOssFileProcess processor = iOssFileProcessor != null ? iOssFileProcessor.getNewInstance(i) : null;
                BufferedReader bufferedReader = targetFileReaderAndWriterMap.getReader(sourceReaderKey);
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

                if (iOssFileProcessor.qiniuException() != null && iOssFileProcessor.qiniuException().code() > 400)
                    throw iOssFileProcessor.qiniuException();

                try {
                    bufferedReader.close();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }

            System.out.println(process + " finished.");
        } catch (IOException ioException) {
            System.out.println("init stream writer or reader failed: " + ioException.getMessage() + ". it need retry.");
            ioException.printStackTrace();
        } finally {
            if (iOssFileProcessor != null) iOssFileProcessor.closeResource();
        }
    }
}