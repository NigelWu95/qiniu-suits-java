package com.qiniu.entry;

import com.qiniu.model.parameter.FileInputParams;
import com.qiniu.service.datasource.FileInput;
import com.qiniu.service.interfaces.ILineProcess;

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
        ILineProcess iLineProcessor = new ProcessorChoice().getFileProcessor(paramFromConfig, args, configFilePath);
        FileInput fileInput = new FileInput(separator, keyIndex, unitLen);
        fileInput.process(maxThreads, sourceFilePath, iLineProcessor);
        if (iLineProcessor != null) iLineProcessor.closeResource();
    }
}
