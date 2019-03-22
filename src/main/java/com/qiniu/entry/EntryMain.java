package com.qiniu.entry;

import com.qiniu.common.Zone;
import com.qiniu.config.CommandArgs;
import com.qiniu.config.FileProperties;
import com.qiniu.datasource.FileInput;
import com.qiniu.datasource.IDataSource;
import com.qiniu.datasource.ListBucket;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EntryMain {

    private static Configuration configuration;

    public static void main(String[] args) throws Exception {
        IEntryParam entryParam = getEntryParam(args);
        configuration = new Configuration(Zone.autoZone());
        // 自定义超时时间
        configuration.connectTimeout = Integer.valueOf(entryParam.getValue("connect-timeout", "60"));
        configuration.readTimeout = Integer.valueOf(entryParam.getValue("read-timeout", "120"));
        configuration.writeTimeout = Integer.valueOf(entryParam.getValue("write-timeout", "60"));

        CommonParams commonParams = new CommonParams(entryParam);
        ILineProcess<Map<String, String>> processor = new ProcessorChoice(entryParam, configuration, commonParams).get();
        IDataSource dataSource = getDataSource(commonParams);
        // 这些参数需要在获取 processor 之后再访问，因为可能由于 ProcessorChoice 的过程对参数的默认值进行修改
        boolean saveTotal = commonParams.getSaveTotal();
        String saveFormat = commonParams.getSaveFormat();
        String saveSeparator = commonParams.getSaveSeparator();
        List<String> rmFields = commonParams.getRmFields();
        if (dataSource != null) {
            dataSource.setResultOptions(saveTotal, saveFormat, saveSeparator, rmFields);
            dataSource.setProcessor(processor);
            dataSource.export();
        }
        if (processor != null) processor.closeResource();
    }

    private static IDataSource getDataSource(CommonParams commonParams) {
        IDataSource dataSource = null;
        String source = commonParams.getSource();
        String savePath = commonParams.getSavePath();
        int threads = commonParams.getThreads();
        int unitLen = commonParams.getUnitLen();
        if ("list".equals(source)) {
            String accessKey = commonParams.getAccessKey();
            String secretKey = commonParams.getSecretKey();
            String bucket = commonParams.getBucket();
            Map<String, String[]> prefixesMap = commonParams.getPrefixesMap();
            List<String> antiPrefixes = commonParams.getAntiPrefixes();
            boolean prefixLeft = commonParams.getPrefixLeft();
            boolean prefixRight = commonParams.getPrefixRight();
            Auth auth = Auth.create(accessKey, secretKey);
            dataSource = new ListBucket(auth, configuration, bucket, threads, unitLen, prefixesMap, antiPrefixes,
                    prefixLeft, prefixRight, savePath);
        } else if ("file".equals(source)) {
            String filePath = commonParams.getPath();
            String parseType = commonParams.getParse();
            String separator = commonParams.getSeparator();
            HashMap<String, String> indexMap = commonParams.getIndexMap();
            dataSource = new FileInput(filePath, parseType, separator, indexMap, unitLen, unitLen, savePath);
        }
        return dataSource;
    }

    private static IEntryParam getEntryParam(String[] args) throws IOException {
        List<String> configFiles = new ArrayList<String>(){{
            add("resources" + System.getProperty("file.separator") + "qiniu.properties");
            add("resources" + System.getProperty("file.separator") + ".qiniu.properties");
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
            if (configFilePath == null) throw new IOException("there is no config file detected.");
            else paramFromConfig = true;
        }

        return paramFromConfig ? new FileProperties(configFilePath) : new CommandArgs(args);
    }
}
