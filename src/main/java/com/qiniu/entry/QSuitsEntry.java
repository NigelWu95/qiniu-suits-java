package com.qiniu.entry;

import com.qiniu.common.Zone;
import com.qiniu.config.ParamsConfig;
import com.qiniu.datasource.FileInput;
import com.qiniu.datasource.IDataSource;
import com.qiniu.datasource.ListBucket;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QSuitsEntry {

    private IEntryParam entryParam;
    private Configuration configuration;
    private CommonParams commonParams;

    public QSuitsEntry(String[] args) throws IOException {
        setEntryParam(args);
        setConfiguration();
        setCommonParams(new CommonParams(entryParam));
    }

    public QSuitsEntry(IEntryParam entryParam) throws IOException {
        this.entryParam = entryParam;
        setConfiguration();
        setCommonParams(new CommonParams(entryParam));
    }

    public QSuitsEntry(IEntryParam entryParam, Configuration configuration) throws IOException {
        this.entryParam = entryParam;
        this.configuration = configuration;
        setCommonParams(new CommonParams(entryParam));
    }

    private void setEntryParam(String[] args) throws IOException {
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
        entryParam = paramFromConfig ? new ParamsConfig(configFilePath) : new ParamsConfig(args);
    }

    private void setConfiguration() {
        this.configuration = new Configuration(Zone.autoZone());
        // 自定义超时时间
        configuration.connectTimeout = Integer.valueOf(entryParam.getValue("connect-timeout", "60"));
        configuration.readTimeout = Integer.valueOf(entryParam.getValue("read-timeout", "120"));
        configuration.writeTimeout = Integer.valueOf(entryParam.getValue("write-timeout", "60"));
    }

    public void setCommonParams(CommonParams commonParams) {
        this.commonParams = commonParams;
    }

    public IEntryParam getEntryParam() {
        return entryParam;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public CommonParams getCommonParams() {
        return commonParams;
    }

    public IDataSource getDataSource() {
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
            dataSource = new ListBucket(auth, configuration, bucket, unitLen, prefixesMap, antiPrefixes, prefixLeft,
                    prefixRight, threads, savePath);
        } else if ("file".equals(source)) {
            String filePath = commonParams.getPath();
            String parseType = commonParams.getParse();
            String separator = commonParams.getSeparator();
            HashMap<String, String> indexMap = commonParams.getIndexMap();
            dataSource = new FileInput(filePath, parseType, separator, indexMap, unitLen, threads, savePath);
        }
        return dataSource;
    }
}
