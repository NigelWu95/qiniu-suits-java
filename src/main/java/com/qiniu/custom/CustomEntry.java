package com.qiniu.custom;

import com.qiniu.common.Zone;
import com.qiniu.config.PropertyConfig;
import com.qiniu.entry.ProcessorChoice;
import com.qiniu.model.parameter.*;
import com.qiniu.service.datasource.FileInput;
import com.qiniu.service.datasource.IDataSource;
import com.qiniu.service.datasource.ListBucket;
import com.qiniu.service.interfaces.IEntryParam;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CustomEntry {

    static private String resultPath;
    static private String resultFormat;
    static private String resultSeparator;
    static private List<String> rmFields;

    public static void main(String[] args) throws Exception {

        IEntryParam entryParam = new PropertyConfig("resources/.qiniu.properties");

        HttpParams httpParams = new HttpParams(entryParam);
        Configuration configuration = new Configuration(Zone.autoZone());
        configuration.connectTimeout = httpParams.getConnectTimeout();
        configuration.readTimeout = httpParams.getReadTimeout();
        configuration.writeTimeout = httpParams.getWriteTimeout();

        FileInputParams fileInputParams = new FileInputParams(entryParam);
        int unitLen = fileInputParams.getUnitLen();
        int threads = fileInputParams.getThreads();
        String filePath = fileInputParams.getFilePath();
        String parseType = fileInputParams.getParseType();
        String separator = fileInputParams.getSeparator();
        boolean saveTotal = fileInputParams.getSaveTotal();
        resultFormat = fileInputParams.getResultFormat();
        resultSeparator = fileInputParams.getResultSeparator();
        resultPath = fileInputParams.getResultPath();
        rmFields = fileInputParams.getRmFields();
        String sourceFilePath = System.getProperty("user.dir") + System.getProperty("file.separator") + filePath;
        Map<String, String> indexMap = fileInputParams.getIndexMap();

//        // parse avinfo from files.
//        indexMap.put("2", "avinfo");
//        ILineProcess<Map<String, String>>  processor = setFopProcessor(entryParam, configuration);

        ILineProcess<Map<String, String>>  processor = setCheckMimeProcessor();

        IDataSource dataSource = new FileInput(sourceFilePath, parseType, separator, indexMap, unitLen, resultPath);
        dataSource.setResultSaveOptions(saveTotal, resultFormat, resultSeparator, rmFields);
        dataSource.export(threads, processor);
        processor.closeResource();
    }

    static private ILineProcess<Map<String, String>> setFopProcessor(IEntryParam entryParam, Configuration configuration)
            throws Exception {
        return new ProcessorChoice(entryParam, configuration).getFileProcessor();
//        QossParams qossParams = new QossParams(entryParam);
//        return new CAvinfoProcess(qossParams.getBucket(), resultPath);
    }

    static private ILineProcess<Map<String, String>> setCheckMimeProcessor() throws Exception {
        return new CCheckMime(resultPath, resultFormat, resultSeparator, rmFields);
    }
}
