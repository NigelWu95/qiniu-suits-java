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

import java.util.List;
import java.util.Map;

public class CFopEntry {

    public static void main(String[] args) throws Exception {

        IEntryParam entryParam = new PropertyConfig("resources/.qiniu-fantx.properties");
        HttpParams httpParams = new HttpParams(entryParam);
        Configuration configuration = new Configuration(Zone.autoZone());
        configuration.connectTimeout = httpParams.getConnectTimeout();
        configuration.readTimeout = httpParams.getReadTimeout();
        configuration.writeTimeout = httpParams.getWriteTimeout();

        QossParams qossParams = new QossParams(entryParam);
        boolean saveTotal = qossParams.getSaveTotal();
        String resultFormat = qossParams.getResultFormat();
        String resultSeparator = qossParams.getResultSeparator();
        String resultPath = qossParams.getResultPath();
        List<String> removeFields = qossParams.getRmFields();
        FileInputParams fileInputParams = new FileInputParams(entryParam);
        Map<String, String> indexMap = fileInputParams.getIndexMap();
        ILineProcess<Map<String, String>> processor;
        processor = new ProcessorChoice(entryParam, configuration).getFileProcessor();
        // parse avinfo from files.
        indexMap.put("2", "avinfo");
        processor = new CAvinfoProcess(qossParams.getBucket(), resultPath);

        int unitLen = qossParams.getUnitLen();
        int threads = qossParams.getThreads();
        String filePath = fileInputParams.getFilePath();
        String parseType = fileInputParams.getParseType();
        String separator = fileInputParams.getSeparator();
        String sourceFilePath = System.getProperty("user.dir") + System.getProperty("file.separator") + filePath;
        IDataSource dataSource = new FileInput(sourceFilePath, parseType, separator, indexMap, unitLen, resultPath);
        dataSource.setResultSaveOptions(saveTotal, resultFormat, resultSeparator, removeFields);
        dataSource.export(threads, processor);
        processor.closeResource();
    }
}
