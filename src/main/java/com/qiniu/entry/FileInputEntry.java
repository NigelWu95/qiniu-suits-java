package com.qiniu.entry;

import com.qiniu.model.parameter.FileInputParams;
import com.qiniu.service.datasource.FileInput;
import com.qiniu.service.interfaces.ILineProcess;

import java.util.Map;

public class FileInputEntry {

    public static void run(boolean paramFromConfig, String[] args, String configFilePath) throws Exception {

        FileInputParams fileInputParams = paramFromConfig ? new FileInputParams(configFilePath) : new FileInputParams(args);
        String filePath = fileInputParams.getFilePath();
        String parseType = fileInputParams.getParseType();
        String separator = fileInputParams.getSeparator();
        String resultFileDir = fileInputParams.getResultFileDir();
        boolean saveTotal = false;
        String resultFormat = fileInputParams.getResultFormat();
        String resultSeparator = fileInputParams.getResultFormat();
        int maxThreads = fileInputParams.getMaxThreads();
        int unitLen = fileInputParams.getUnitLen();
        String sourceFilePath = System.getProperty("user.dir") + System.getProperty("file.separator") + filePath;
        ILineProcess<Map<String, String>> lineProcessor = new ProcessorChoice().getFileProcessor(paramFromConfig,
                args, configFilePath);
        Map<String, String> infoIndexMap = new InputInfoParser().getInfoIndexMap(fileInputParams);
        FileInput fileInput = new FileInput(parseType, separator, infoIndexMap, 3, unitLen, resultFileDir);
        fileInput.process(maxThreads, sourceFilePath, lineProcessor);
        lineProcessor.closeResource();
    }
}
