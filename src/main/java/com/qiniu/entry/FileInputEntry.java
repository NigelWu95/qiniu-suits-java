package com.qiniu.entry;

import com.qiniu.model.parameter.FileInputParams;
import com.qiniu.service.datasource.FileInput;
import com.qiniu.service.interfaces.IEntryParam;
import com.qiniu.service.interfaces.ILineProcess;

import java.util.ArrayList;
import java.util.Map;

public class FileInputEntry {

    public static void run(IEntryParam entryParam) throws Exception {

        FileInputParams fileInputParams = new FileInputParams(entryParam);
        String filePath = fileInputParams.getFilePath();
        String parseType = fileInputParams.getParseType();
        String separator = fileInputParams.getSeparator();
        boolean saveTotal = fileInputParams.getSaveTotal();
        String resultFormat = fileInputParams.getResultFormat();
        String resultSeparator = fileInputParams.getResultFormat();
        String resultPath = fileInputParams.getResultPath();
        int maxThreads = fileInputParams.getMaxThreads();
        int unitLen = fileInputParams.getUnitLen();
        String sourceFilePath = System.getProperty("user.dir") + System.getProperty("file.separator") + filePath;
        ILineProcess<Map<String, String>> lineProcessor = new ProcessorChoice(entryParam).getFileProcessor();
        String processName = lineProcessor != null ? lineProcessor.getProcessName() : "";
        Map<String, String> infoIndexMap = new InputInfoParser().getInfoIndexMap(fileInputParams, processName);
        FileInput fileInput = new FileInput(parseType, separator, infoIndexMap, unitLen, resultPath);
        // TODO
        fileInput.setResultSaveOptions(resultFormat, resultSeparator, new ArrayList<>());
        fileInput.process(maxThreads, sourceFilePath, lineProcessor);
        if (lineProcessor != null) lineProcessor.closeResource();
    }
}
