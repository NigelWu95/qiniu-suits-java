package com.qiniu.entry;

import com.qiniu.common.Zone;
import com.qiniu.config.CommandArgs;
import com.qiniu.config.PropertyConfig;
import com.qiniu.model.parameter.CommonParams;
import com.qiniu.model.parameter.FileInputParams;
import com.qiniu.model.parameter.ListBucketParams;
import com.qiniu.service.datasource.FileInput;
import com.qiniu.service.datasource.IDataSource;
import com.qiniu.service.datasource.ListBucket;
import com.qiniu.service.interfaces.IEntryParam;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EntryMain {

    public static void main(String[] args) throws Exception {

        List<String> configFiles = new ArrayList<String>(){{
            add("resources/qiniu.properties");
            add("resources/.qiniu.properties");
        }};
        boolean paramFromConfig = true;
        if (args != null && args.length > 0) {
            if (args[0].startsWith("-config=")) configFiles.add(args[0].split("=")[1]);
            else paramFromConfig = false;
        }
        String configFilePath = null;
        if (paramFromConfig) {
            for (int i = configFiles.size() - 1; i >= 0; i--) {
                File file = new File(configFiles.get(i));
                if (file.exists()) {
                    configFilePath = configFiles.get(i);
                    break;
                }
            }
            if (configFilePath == null) throw new Exception("there is no config file detected.");
            else paramFromConfig = true;
        }

        IEntryParam entryParam = paramFromConfig ? new PropertyConfig(configFilePath) : new CommandArgs(args);
        String sourceType = entryParam.getParamValue("source-type");
        ILineProcess<Map<String, String>> processor = new ProcessorChoice(entryParam).getFileProcessor();
        IDataSource dataSource = null;
        CommonParams commonParams = new CommonParams(entryParam);
        boolean saveTotal = commonParams.getSaveTotal();
        String resultFormat = commonParams.getResultFormat();
        String resultSeparator = commonParams.getResultFormat();
        String resultPath = commonParams.getResultPath();
        int unitLen = commonParams.getUnitLen();
        int threads = commonParams.getThreads();
        List<String> removeFields = commonParams.getRmFields();

        if ("list".equals(sourceType)) {
            ListBucketParams listBucketParams = new ListBucketParams(entryParam);
            String accessKey = listBucketParams.getAccessKey();
            String secretKey = listBucketParams.getSecretKey();
            String bucket = listBucketParams.getBucket();
            String customPrefix = listBucketParams.getCustomPrefix();
            List<String> antiPrefix = listBucketParams.getAntiPrefix();
            Auth auth = Auth.create(accessKey, secretKey);
            Configuration configuration = new Configuration(Zone.autoZone());
            dataSource = new ListBucket(auth, configuration, bucket, unitLen, customPrefix, antiPrefix, 3, resultPath);
        } else if ("file".equals(sourceType)) {
            FileInputParams fileInputParams = new FileInputParams(entryParam);
            String filePath = fileInputParams.getFilePath();
            String parseType = fileInputParams.getParseType();
            String separator = fileInputParams.getSeparator();
            Map<String, String> indexMap = fileInputParams.getIndexMap();
            String sourceFilePath = System.getProperty("user.dir") + System.getProperty("file.separator") + filePath;
            dataSource = new FileInput(sourceFilePath, parseType, separator, indexMap, unitLen, resultPath);
        }

        if (dataSource != null) {
            if (saveTotal) dataSource.setResultSaveOptions(resultFormat, resultSeparator, removeFields);
            dataSource.exportData(threads, processor);
        }
        if (processor != null) processor.closeResource();
    }
}
