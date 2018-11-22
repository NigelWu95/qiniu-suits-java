package com.qiniu.entry;

import com.qiniu.model.*;
import com.qiniu.service.datasource.FileInput;
import com.qiniu.service.interfaces.IOssFileProcess;

public class FileInputProcess {

    public static void run(boolean paramFromConfig, String[] args, String configFilePath) throws Exception {

        FileInputParams fileInputParams = paramFromConfig ?
                new FileInputParams(configFilePath) : new FileInputParams(args);
        String filePath = fileInputParams.getFilePath();
        String separator = fileInputParams.getSeparator();
        int keyIndex = fileInputParams.getKeyIndex();
        int maxThreads = fileInputParams.getMaxThreads();
        int unitLen = fileInputParams.getUnitLen();
        String sourceFilePath = System.getProperty("user.dir") + System.getProperty("file.separator") + filePath;
        IOssFileProcess iOssFileProcessor = ProcessorChoice.getFileProcessor(paramFromConfig, args, configFilePath);
        FileInput fileInput = new FileInput(separator, keyIndex, unitLen, 3);
        fileInput.process(maxThreads, sourceFilePath, iOssFileProcessor);
        if (iOssFileProcessor != null) iOssFileProcessor.closeResource();
    }
}
