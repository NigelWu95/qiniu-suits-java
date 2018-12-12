package com.qiniu.entry;

import com.qiniu.model.parameter.FileInputParams;
import com.qiniu.model.parameter.InfoMapParams;
import com.qiniu.service.datasource.FileInput;
import com.qiniu.service.interfaces.ILineProcess;

import java.util.Map;

public class FileInputEntry {

    public static void run(boolean paramFromConfig, String[] args, String configFilePath) throws Exception {

        FileInputParams fileInputParams = paramFromConfig ? new FileInputParams(configFilePath) : new FileInputParams(args);
        InfoMapParams infoMapParams = paramFromConfig ? new InfoMapParams(configFilePath) : new InfoMapParams(args);
        String filePath = fileInputParams.getFilePath();
        String parserType = fileInputParams.getParserType();
        String separator = fileInputParams.getSeparator();
        int maxThreads = fileInputParams.getMaxThreads();
        int unitLen = fileInputParams.getUnitLen();
        String sourceFilePath = System.getProperty("user.dir") + System.getProperty("file.separator") + filePath;
        ILineProcess<Map<String, String>> iLineProcessor = new ProcessorChoice().getFileProcessor(paramFromConfig,
                args, configFilePath);
        FileInput fileInput = new FileInput(parserType, separator, unitLen, infoMapParams);
        fileInput.process(maxThreads, sourceFilePath, iLineProcessor);
        if (iLineProcessor != null) iLineProcessor.closeResource();
    }
}
