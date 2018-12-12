package com.qiniu.entry;

import com.qiniu.model.parameter.FileInputParams;
import com.qiniu.model.parameter.InfoMapParams;
import com.qiniu.service.datasource.FileInput;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.process.FileInputResultProcess;

import java.util.Map;

public class FileInputEntry {

    public static void run(boolean paramFromConfig, String[] args, String configFilePath) throws Exception {

        FileInputParams fileInputParams = paramFromConfig ? new FileInputParams(configFilePath) : new FileInputParams(args);
        InfoMapParams infoMapParams = paramFromConfig ? new InfoMapParams(configFilePath) : new InfoMapParams(args);
        String filePath = fileInputParams.getFilePath();
        String parserType = fileInputParams.getParserType();
        String inputSeparator = fileInputParams.getSeparator();
        String resultFormat = fileInputParams.getResultFormat();
        String resultSeparator = fileInputParams.getResultFormat();
        String resultFileDir = fileInputParams.getResultFileDir();
        boolean saveTotal = false;
        int maxThreads = fileInputParams.getMaxThreads();
        int unitLen = fileInputParams.getUnitLen();
        String sourceFilePath = System.getProperty("user.dir") + System.getProperty("file.separator") + filePath;
        ILineProcess<Map<String, String>> lineProcessor = new ProcessorChoice().getFileProcessor(paramFromConfig,
                args, configFilePath);
        FileInput fileInput = new FileInput(unitLen);
        ILineProcess<String> inputResultProcessor = new FileInputResultProcess(parserType, inputSeparator, infoMapParams,
                resultFormat, resultSeparator, resultFileDir, saveTotal);
        inputResultProcessor.setNextProcessor(lineProcessor);
        fileInput.process(maxThreads, sourceFilePath, inputResultProcessor);
        inputResultProcessor.closeResource();
    }
}
