package com.qiniu.custom;

import com.qiniu.config.PropertyConfig;
import com.qiniu.entry.ProcessorChoice;
import com.qiniu.model.parameter.*;
import com.qiniu.service.datasource.FileInput;
import com.qiniu.service.datasource.IDataSource;
import com.qiniu.service.interfaces.IEntryParam;
import com.qiniu.service.interfaces.ILineProcess;

import java.util.List;
import java.util.Map;

public class CFopEntry {

    public static void main(String[] args) throws Exception {

        IEntryParam entryParam = new PropertyConfig("resources/.qiniu-fantx.properties");
        QossParams qossParams = new QossParams(entryParam);
        boolean saveTotal = qossParams.getSaveTotal();
        String resultFormat = qossParams.getResultFormat();
        String resultSeparator = qossParams.getResultSeparator();
        String resultPath = qossParams.getResultPath();
        int unitLen = qossParams.getUnitLen();
        int threads = qossParams.getThreads();
        List<String> removeFields = qossParams.getRmFields();
        FileInputParams fileInputParams = new FileInputParams(entryParam);
        String filePath = fileInputParams.getFilePath();
        String parseType = fileInputParams.getParseType();
        String separator = fileInputParams.getSeparator();
        Map<String, String> indexMap = fileInputParams.getIndexMap();
        String sourceFilePath = System.getProperty("user.dir") + System.getProperty("file.separator") + filePath;
        IDataSource dataSource = new FileInput(sourceFilePath, parseType, separator, indexMap, unitLen, resultPath);
        ILineProcess<Map<String, String>> processor;
//        processor = new ProcessorChoice(entryParam).getFileProcessor();
        // parse avinfo from files.
        processor = new CAvinfoProcess(qossParams.getBucket(), resultPath);
        // filter pfop result
//        processor = new PfopResultProcess(resultFileDir);
        if (saveTotal) dataSource.setResultSaveOptions(resultFormat, resultSeparator, removeFields);
        dataSource.exportData(threads, processor);
        processor.closeResource();
    }
}
