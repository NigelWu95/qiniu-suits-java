package com.qiniu.entries;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.Zone;
import com.qiniu.interfaces.ILineParser;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.model.*;
import com.qiniu.service.fileline.SplitLineParser;
import com.qiniu.service.impl.BucketCopyProcess;
import com.qiniu.service.impl.ChangeStatusProcess;
import com.qiniu.service.impl.ChangeTypeProcess;
import com.qiniu.service.impl.UpdateLifecycleProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
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
        String accessKey = sourceFileParams.getAccessKey();
        String secretKey = sourceFileParams.getSecretKey();
        String process = sourceFileParams.getProcess();
        boolean processBatch = sourceFileParams.getProcessBatch();
        String resultFormat = sourceFileParams.getResultFormat();
        String resultFileDir = sourceFileParams.getResultFileDir();
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

        IOssFileProcess iOssFileProcessor = null;
        Auth auth = Auth.create(accessKey, secretKey);
        Configuration configuration = new Configuration(Zone.autoZone());

        switch (process) {
            case "status": {
                FileStatusParams fileStatusParams = paramFromConfig ? new FileStatusParams(configFilePath) : new FileStatusParams(args);
                iOssFileProcessor = new ChangeStatusProcess(auth, configuration, fileStatusParams.getBucket(), fileStatusParams.getTargetStatus(),
                        resultFileDir);
                break;
            }
            case "type": {
                FileTypeParams fileTypeParams = paramFromConfig ? new FileTypeParams(configFilePath) : new FileTypeParams(args);
                iOssFileProcessor = new ChangeTypeProcess(auth, configuration, fileTypeParams.getBucket(), fileTypeParams.getTargetType(),
                        resultFileDir);
                break;
            }
            case "copy": {
                FileCopyParams fileCopyParams = paramFromConfig ? new FileCopyParams(configFilePath) : new FileCopyParams(args);
                accessKey = "".equals(fileCopyParams.getAKey()) ? accessKey : fileCopyParams.getAKey();
                secretKey = "".equals(fileCopyParams.getSKey()) ? secretKey : fileCopyParams.getSKey();
                iOssFileProcessor = new BucketCopyProcess(Auth.create(accessKey, secretKey), configuration, fileCopyParams.getSourceBucket(),
                        fileCopyParams.getTargetBucket(), fileCopyParams.getTargetKeyPrefix(), resultFileDir);
                break;
            }
            case "lifecycle": {
                LifecycleParams lifecycleParams = paramFromConfig ? new LifecycleParams(configFilePath) : new LifecycleParams(args);
                iOssFileProcessor = new UpdateLifecycleProcess(Auth.create(accessKey, secretKey), configuration, lifecycleParams.getBucket(),
                        lifecycleParams.getDays(), resultFileDir);
                break;
            }
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

            System.out.println(process + " completed for: " + resultFileDir);
        } catch (IOException ioException) {
            System.out.println("init stream writer or reader failed: " + ioException.getMessage() + ". it need retry.");
            ioException.printStackTrace();
        } finally {
            if (iOssFileProcessor != null) iOssFileProcessor.closeResource();
        }
    }
}