package com.qiniu.custom;

import com.qiniu.common.Zone;
import com.qiniu.config.PropertyConfig;
import com.qiniu.entry.ProcessorChoice;
import com.qiniu.model.parameter.*;
import com.qiniu.service.datasource.FileInput;
import com.qiniu.service.datasource.IDataSource;
import com.qiniu.service.interfaces.IEntryParam;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CustomEntry {

    static private boolean saveTotal;
    static private String resultFormat;
    static private String resultSeparator;
    static private String resultPath;
    static private List<String> removeFields;
    static private ILineProcess<Map<String, String>> processor;

    public static void main(String[] args) throws Exception {

        IEntryParam entryParam = new PropertyConfig("resources/.qiniu-fantx.properties");

        FileInputParams fileInputParams = new FileInputParams(entryParam);
        Map<String, String> indexMap = fileInputParams.getIndexMap();
        // parse avinfo from files.
        indexMap.put("2", "avinfo");
        setFopProcessor(entryParam);

        int unitLen = fileInputParams.getUnitLen();
        int threads = fileInputParams.getThreads();
        String filePath = fileInputParams.getFilePath();
        String parseType = fileInputParams.getParseType();
        String separator = fileInputParams.getSeparator();
        String sourceFilePath = System.getProperty("user.dir") + System.getProperty("file.separator") + filePath;
        IDataSource dataSource = new FileInput(sourceFilePath, parseType, separator, indexMap, unitLen, resultPath);
        dataSource.setResultSaveOptions(saveTotal, resultFormat, resultSeparator, removeFields);
        dataSource.export(threads, processor);
        processor.closeResource();
    }

    static private void setFopProcessor(IEntryParam entryParam) throws Exception {

        HttpParams httpParams = new HttpParams(entryParam);
        Configuration configuration = new Configuration(Zone.autoZone());
        configuration.connectTimeout = httpParams.getConnectTimeout();
        configuration.readTimeout = httpParams.getReadTimeout();
        configuration.writeTimeout = httpParams.getWriteTimeout();

        QossParams qossParams = new QossParams(entryParam);
        saveTotal = qossParams.getSaveTotal();
        resultFormat = qossParams.getResultFormat();
        resultSeparator = qossParams.getResultSeparator();
        resultPath = qossParams.getResultPath();
        removeFields = qossParams.getRmFields();
        processor = new ProcessorChoice(entryParam, configuration).getFileProcessor();
        processor = new CAvinfoProcess(qossParams.getBucket(), resultPath);
    }
}
