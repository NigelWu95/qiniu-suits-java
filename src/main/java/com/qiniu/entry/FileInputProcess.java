package com.qiniu.entry;

import com.qiniu.model.*;
import com.qiniu.service.datasource.FileInput;
import com.qiniu.service.interfaces.IOssFileProcess;

public class FileInputProcess {

    public static void run(boolean paramFromConfig, String[] args, String configFilePath) throws Exception {

        SourceFileParams sourceFileParams = paramFromConfig ?
                new SourceFileParams(configFilePath) : new SourceFileParams(args);
        String filePath = sourceFileParams.getFilePath();
        String separator = sourceFileParams.getSeparator();
        int keyIndex = sourceFileParams.getKeyIndex();
        boolean processBatch = sourceFileParams.getProcessBatch();
        int maxThreads = sourceFileParams.getMaxThreads();
        String sourceFilePath = System.getProperty("user.dir") + System.getProperty("file.separator") + filePath;
        IOssFileProcess iOssFileProcessor = ProcessChoice.getFileProcessor(paramFromConfig, args, configFilePath);
        FileInput fileInput = new FileInput(separator, keyIndex, 3);
        fileInput.process(maxThreads, sourceFilePath, iOssFileProcessor, processBatch);
        if (iOssFileProcessor != null) iOssFileProcessor.closeResource();
    }
}
