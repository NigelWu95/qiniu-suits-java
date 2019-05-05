package com.qiniu.entry;

import com.aliyun.oss.ClientConfiguration;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.region.Region;
import com.qiniu.common.Constants;
import com.qiniu.common.Zone;
import com.qiniu.config.ParamsConfig;
import com.qiniu.config.PropertiesFile;
import com.qiniu.datasource.*;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.process.filtration.*;
import com.qiniu.process.qdora.*;
import com.qiniu.process.qoss.*;
import com.qiniu.storage.Configuration;
import com.qiniu.util.ProcessUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class QSuitsEntry {

    private IEntryParam entryParam;
    private CommonParams commonParams;
    private Configuration qiniuConfig;
    private ClientConfig tenClientConfig;
    private ClientConfiguration aliClientConfig;
    private String source;
    private String qiniuAccessKey;
    private String qiniuSecretKey;
    private String bucket;
    private Map<String, String> indexMap;
    private int unitLen;
    private int threads;
    private boolean saveTotal;
    private List<String> rmFields;
    private String process;
    private int retryTimes;
    private String savePath;
    private String saveFormat;
    private String saveSeparator;

    public QSuitsEntry(String[] args) throws Exception {
        setEntryParam(args);
        this.commonParams = new CommonParams(entryParam);
        setMembers();
    }

    public QSuitsEntry(IEntryParam entryParam) throws Exception {
        this.entryParam = entryParam;
        this.commonParams = new CommonParams(entryParam);
        setMembers();
    }

    public void UpdateEntry(IEntryParam entryParam) throws Exception {
        this.entryParam = entryParam;
        this.commonParams = new CommonParams(entryParam);
        setMembers();
    }

    public void UpdateEntry(CommonParams commonParams) {
        this.commonParams = commonParams;
        setMembers();
    }

    public void setQiniuConfig(Configuration configuration) throws IOException {
        if (configuration == null) throw new IOException("the configuration can not be null when you set it.");
        this.qiniuConfig = configuration;
    }

    public void setTenClientConfig(ClientConfig clientConfig) throws IOException {
        if (clientConfig == null) throw new IOException("the configuration can not be null when you set it.");
        this.tenClientConfig = clientConfig;
    }

    public void setAliClientConfig(ClientConfiguration clientConfig) throws IOException {
        if (clientConfig == null) throw new IOException("the configuration can not be null when you set it.");
        this.aliClientConfig = clientConfig;
    }

    private void setMembers() {
        this.source = commonParams.getSource();
        this.qiniuAccessKey = commonParams.getQiniuAccessKey();
        this.qiniuSecretKey = commonParams.getQiniuSecretKey();
        this.bucket = commonParams.getBucket();
        this.indexMap = commonParams.getIndexMap();
        this.unitLen = commonParams.getUnitLen();
        this.threads = commonParams.getThreads();
        this.saveTotal = commonParams.getSaveTotal();
        this.rmFields = commonParams.getRmFields();
        this.process = commonParams.getProcess();
        this.retryTimes = commonParams.getRetryTimes();
        this.savePath = commonParams.getSavePath() + commonParams.getSaveTag();
        this.saveFormat = commonParams.getSaveFormat();
        this.saveSeparator = commonParams.getSaveSeparator();
    }

    private void setEntryParam(String[] args) throws IOException {
        List<String> configFiles = new ArrayList<String>(){{
            add("resources" + System.getProperty("file.separator") + "application.config");
            add("resources" + System.getProperty("file.separator") + ".application.config");
            add("resources" + System.getProperty("file.separator") + ".application.properties");
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
        if (paramFromConfig) {
            if (configFilePath.endsWith(".properties")) {
                entryParam = new ParamsConfig(new PropertiesFile(configFilePath).getProperties());
            } else {
                entryParam = new ParamsConfig(configFilePath);
            }
        } else {
            entryParam = new ParamsConfig(args);
        }
    }

    public IEntryParam getEntryParam() {
        return entryParam;
    }

    public CommonParams getCommonParams() {
        return commonParams;
    }

    public Configuration getQiniuConfig() {
        return qiniuConfig == null ? getDefaultQiniuConfig() : qiniuConfig;
    }

    private Configuration getDefaultQiniuConfig() {
        Configuration configuration = new Configuration(Zone.autoZone());
        if (commonParams.getConnectTimeout() > Constants.CONNECT_TIMEOUT)
            configuration.connectTimeout = commonParams.getConnectTimeout();
        if (commonParams.getReadTimeout() > Constants.READ_TIMEOUT)
            configuration.readTimeout = commonParams.getReadTimeout();
        if (commonParams.getRequestTimeout() > Constants.WRITE_TIMEOUT)
            configuration.writeTimeout = commonParams.getRequestTimeout();
        return configuration;
    }

    public ClientConfig getTenClientConfig() {
        return tenClientConfig == null ? getDefaultTenClientConfig() : tenClientConfig;
    }

    private ClientConfig getDefaultTenClientConfig() {
        ClientConfig clientConfig = new ClientConfig(new Region(commonParams.getRegionName()));
        if (1000 * commonParams.getConnectTimeout() > clientConfig.getConnectionTimeout())
            clientConfig.setConnectionTimeout(1000 * commonParams.getConnectTimeout());
        if (1000 * commonParams.getReadTimeout() > clientConfig.getSocketTimeout())
            clientConfig.setSocketTimeout(1000 * commonParams.getReadTimeout());
        if (1000 * commonParams.getRequestTimeout() > clientConfig.getConnectionRequestTimeout())
            clientConfig.setConnectionRequestTimeout(1000 * commonParams.getRequestTimeout());
        return clientConfig;
    }

    public ClientConfiguration getAliClientConfig() {
        return aliClientConfig == null ? getDefaultAliClientConfig() : aliClientConfig;
    }

    private ClientConfiguration getDefaultAliClientConfig() {
        ClientConfiguration clientConfig = new ClientConfiguration();
        if (1000 * commonParams.getConnectTimeout() > clientConfig.getConnectionTimeout())
            clientConfig.setConnectionTimeout(1000 * commonParams.getConnectTimeout());
        if (1000 * commonParams.getReadTimeout() > clientConfig.getSocketTimeout())
            clientConfig.setSocketTimeout(1000 * commonParams.getReadTimeout());
        if (1000 * commonParams.getRequestTimeout() > clientConfig.getConnectionRequestTimeout())
            clientConfig.setConnectionRequestTimeout(1000 * commonParams.getRequestTimeout());
        return clientConfig;
    }

    public IDataSource getDataSource() {
        if ("qiniu".equals(source)) {
            return getQiniuOssContainer();
        } else if ("tencent".equals(source)) {
            return getTenOssContainer();
        } else if ("aliyun".equals(source)) {
            return getAliOssContainer();
        } else if ("local".equals(source)) {
            return getLocalFileContainer();
        } else {
            return null;
        }
    }

    public LocalFileContainer getLocalFileContainer() {
        String filePath = commonParams.getPath();
        String parseFormat = commonParams.getParse();
        String separator = commonParams.getSeparator();
        String addKeyPrefix = commonParams.getRmKeyPrefix();
        String rmKeyPrefix = commonParams.getRmKeyPrefix();
        LocalFileContainer localFileContainer = new LocalFileContainer(filePath, parseFormat, separator, addKeyPrefix,
                rmKeyPrefix, indexMap, unitLen, threads);
        localFileContainer.setSaveOptions(savePath, saveTotal, saveFormat, saveSeparator, rmFields);
        localFileContainer.setRetryTimes(retryTimes);
        return localFileContainer;
    }

    public QiniuOssContainer getQiniuOssContainer() {
        Map<String, String[]> prefixesMap = commonParams.getPrefixesMap();
        List<String> antiPrefixes = commonParams.getAntiPrefixes();
        boolean prefixLeft = commonParams.getPrefixLeft();
        boolean prefixRight = commonParams.getPrefixRight();
        if (qiniuConfig == null) qiniuConfig = getDefaultQiniuConfig();
        QiniuOssContainer qiniuOssContainer = new QiniuOssContainer(qiniuAccessKey, qiniuSecretKey, qiniuConfig,
                bucket, antiPrefixes, prefixesMap, prefixLeft, prefixRight, indexMap, unitLen, threads);
        qiniuOssContainer.setSaveOptions(savePath, saveTotal, saveFormat, saveSeparator, rmFields);
        qiniuOssContainer.setRetryTimes(retryTimes);
        return qiniuOssContainer;
    }

    public TenOssContainer getTenOssContainer() {
        String secretId = commonParams.getTencentSecretId();
        String secretKey = commonParams.getTencentSecretKey();
        Map<String, String[]> prefixesMap = commonParams.getPrefixesMap();
        List<String> antiPrefixes = commonParams.getAntiPrefixes();
        boolean prefixLeft = commonParams.getPrefixLeft();
        boolean prefixRight = commonParams.getPrefixRight();
        if (tenClientConfig == null) tenClientConfig = getDefaultTenClientConfig();
        TenOssContainer tenOssContainer = new TenOssContainer(secretId, secretKey, tenClientConfig, bucket,
                antiPrefixes, prefixesMap, prefixLeft, prefixRight, indexMap, unitLen, threads);
        tenOssContainer.setSaveOptions(savePath, saveTotal, saveFormat, saveSeparator, rmFields);
        tenOssContainer.setRetryTimes(retryTimes);
        return tenOssContainer;
    }

    public AliOssContainer getAliOssContainer() {
        String accessId = commonParams.getAliyunAccessId();
        String accessSecret = commonParams.getAliyunAccessSecret();
        String endPoint = "http://" + commonParams.getRegionName() + ".aliyuncs.com";
        Map<String, String[]> prefixesMap = commonParams.getPrefixesMap();
        List<String> antiPrefixes = commonParams.getAntiPrefixes();
        boolean prefixLeft = commonParams.getPrefixLeft();
        boolean prefixRight = commonParams.getPrefixRight();
        if (aliClientConfig == null) aliClientConfig = getDefaultAliClientConfig();
        AliOssContainer aliOssContainer = new AliOssContainer(accessId, accessSecret, aliClientConfig, endPoint, bucket,
                antiPrefixes, prefixesMap, prefixLeft, prefixRight, indexMap, unitLen, threads);
        aliOssContainer.setSaveOptions(savePath, saveTotal, saveFormat, saveSeparator, rmFields);
        aliOssContainer.setRetryTimes(retryTimes);
        return aliOssContainer;
    }

    public ILineProcess<Map<String, String>> getProcessor() throws Exception {
        ILineProcess<Map<String, String>> nextProcessor = process == null ? null : whichNextProcessor();
        if (nextProcessor != null) {
            if (ProcessUtils.canBatch(nextProcessor.getProcessName())) nextProcessor.setBatchSize(commonParams.getBatchSize());
            // 为了保证程序出现因网络等原因产生的非预期异常时正常运行需要设置重试次数，filter 操作不需要重试
            nextProcessor.setRetryTimes(retryTimes);
        }
        ILineProcess<Map<String, String>> processor;
        BaseFilter<Map<String, String>> baseFilter = commonParams.getBaseFilter();
        SeniorFilter<Map<String, String>> seniorFilter = commonParams.getSeniorFilter();
        if (baseFilter != null || seniorFilter != null) {
            List<String> rmFields = Arrays.asList(entryParam.getValue("rm-fields", "").split(","));
            processor = new MapProcess(baseFilter, seniorFilter, savePath, saveFormat, saveSeparator, rmFields);
            processor.setNextProcessor(nextProcessor);
        } else {
            if ("filter".equals(process)) {
                throw new Exception("please set the correct filter conditions.");
            } else {
                processor = nextProcessor;
            }
        }
        return processor;
    }

    private ILineProcess<Map<String, String>> whichNextProcessor() throws Exception {
        ILineProcess<Map<String, String>> processor = null;
        if (qiniuConfig == null) qiniuConfig = getDefaultQiniuConfig();
        switch (process) {
            case "status": processor = getChangeStatus(); break;
            case "type": processor = getChangeType(); break;
            case "lifecycle": processor = getChangeLifecycle(); break;
            case "copy": processor = getCopyFile(); break;
            case "move":
            case "rename": processor = getMoveFile(); break;
            case "delete": processor = getDeleteFile(); break;
            case "asyncfetch": processor = getAsyncFetch(); break;
            case "avinfo": processor = getQueryAvinfo(); break;
            case "pfopcmd": processor = getPfopCommand(); break;
            case "pfop": processor = getPfop(); break;
            case "pfopresult": processor = getPfopResult(); break;
            case "qhash": processor = getQueryHash(); break;
            case "stat": processor = getStatFile(); break;
            case "privateurl": processor = getPrivateUrl(); break;
            case "mirror": processor = getMirrorFile(); break;
            case "exportts": processor = getExportTs(); break;
        }
        return processor;
    }

    private ILineProcess<Map<String, String>> getChangeStatus() throws IOException {
        String status = commonParams.checked(entryParam.getValue("status"), "status", "[01]");
        return new ChangeStatus(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, Integer.valueOf(status), savePath);
    }

    private ILineProcess<Map<String, String>> getChangeType() throws IOException {
        String type = commonParams.checked(entryParam.getValue("type"), "type", "[01]");
        return new ChangeType(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, Integer.valueOf(type), savePath);
    }

    private ILineProcess<Map<String, String>> getChangeLifecycle() throws IOException {
        String days = commonParams.checked(entryParam.getValue("days"), "days", "[01]");
        return new ChangeLifecycle(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, Integer.valueOf(days), savePath);
    }

    private ILineProcess<Map<String, String>> getCopyFile() throws IOException {
        String toBucket = entryParam.getValue("to-bucket");
        String newKeyIndex = commonParams.containIndex("newKey") ? "newKey" : null;
        String addPrefix = entryParam.getValue("add-prefix", null);
        String rmPrefix = entryParam.getValue("rm-prefix", null);
        return new CopyFile(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, toBucket, newKeyIndex, addPrefix,
                rmPrefix, savePath);
    }

    private ILineProcess<Map<String, String>> getMoveFile() throws IOException {
        String toBucket = entryParam.getValue("to-bucket", null);
        if ("move".equals(process) && toBucket == null) throw new IOException("no incorrect to-bucket, please set it.");
        String newKeyIndex = commonParams.containIndex("newKey") ? "newKey" : null;
        String addPrefix = entryParam.getValue("add-prefix", null);
        String force = entryParam.getValue("prefix-force", null);
        force = commonParams.checked(force, "prefix-force", "(true|false)");
        String rmPrefix = entryParam.getValue("rm-prefix", null);
        return new MoveFile(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, toBucket, newKeyIndex, addPrefix,
                Boolean.valueOf(force), rmPrefix, savePath);
    }

    private ILineProcess<Map<String, String>> getDeleteFile() throws IOException {
        return new DeleteFile(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, savePath);
    }

    private ILineProcess<Map<String, String>> getAsyncFetch() throws IOException {
        String toBucket = entryParam.getValue("to-bucket");
        String domain = entryParam.getValue("domain", null);
        String protocol = entryParam.getValue("protocol", "http");
        protocol = commonParams.checked(protocol, "protocol", "https?");
        String urlIndex = commonParams.containIndex("url") ? "url" : null;
        String addPrefix = entryParam.getValue("add-prefix", null);
        String host = entryParam.getValue("host", null);
        String md5Index = commonParams.containIndex("md5") ? "md5" : null;
        String callbackUrl = entryParam.getValue("callback-url", null);
        String callbackBody = entryParam.getValue("callback-body", null);
        String callbackBodyType = entryParam.getValue("callback-body-type", null);
        String callbackHost = entryParam.getValue("callback-host", null);
        String type = entryParam.getValue("file-type", "0");
        String ignore = entryParam.getValue("ignore-same-key", "false");
        ignore = commonParams.checked(ignore, "ignore-same-key", "(true|false)");
        ILineProcess<Map<String, String>> processor = new AsyncFetch(qiniuAccessKey, qiniuSecretKey, qiniuConfig,
                toBucket, domain, protocol, urlIndex, addPrefix, savePath);
        if (host != null || md5Index != null || callbackUrl != null || callbackBody != null || callbackBodyType != null
                || callbackHost != null || "1".equals(type) || "true".equals(ignore)) {
            ((AsyncFetch) processor).setFetchArgs(host, md5Index, callbackUrl, callbackBody,
                    callbackBodyType, callbackHost, Integer.valueOf(type), Boolean.valueOf(ignore));
        }
        return processor;
    }

    private ILineProcess<Map<String, String>> getQueryAvinfo() throws IOException {
        String domain = entryParam.getValue("domain", null);
        String protocol = entryParam.getValue("protocol", "http");
        protocol = commonParams.checked(protocol, "protocol", "https?");
        String urlIndex = commonParams.containIndex("url") ? "url" : null;
        return new QueryAvinfo(qiniuConfig, domain, protocol, urlIndex, savePath);
    }

    private ILineProcess<Map<String, String>> getPfopCommand() throws IOException {
        String avinfoIndex = commonParams.containIndex("avinfo") ? "avinfo" : null;
        String configJson = entryParam.getValue("pfop-config");
        String duration = entryParam.getValue("duration", "false");
        duration = commonParams.checked(duration, "duration", "(true|false)");
        String size = entryParam.getValue("size", "false");
        size = commonParams.checked(size, "size", "(true|false)");
        return new PfopCommand(avinfoIndex, configJson, Boolean.valueOf(duration), Boolean.valueOf(size), savePath);
    }

    private ILineProcess<Map<String, String>> getPfop() throws IOException {
        String fopsIndex = commonParams.containIndex("fops") ? "fops" : null;
        String forcePublic = entryParam.getValue("force-public", "false");
        String pipeline = entryParam.getValue("pipeline", null);
        if (pipeline == null && !"true".equals(forcePublic)) {
            throw new IOException("please set pipeline, if you don't want to use" +
                    " private pipeline, please set the force-public as true.");
        }
        String configJson = entryParam.getValue("pfop-config", null);
        return new QiniuPfop(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, pipeline, fopsIndex, configJson, savePath);
    }

    private ILineProcess<Map<String, String>> getPfopResult() throws IOException {
        String protocol = entryParam.getValue("protocol", "http");
        protocol = commonParams.checked(protocol, "protocol", "https?");
        String persistentIdIndex = commonParams.containIndex("pid") ? "pid" : null;
        return new QueryPfopResult(qiniuConfig, protocol, persistentIdIndex, savePath);
    }

    private ILineProcess<Map<String, String>> getQueryHash() throws IOException {
        String domain = entryParam.getValue("domain", null);
        String algorithm = entryParam.getValue("algorithm", "md5");
        algorithm = commonParams.checked(algorithm, "algorithm", "(md5|sha1)");
        String protocol = entryParam.getValue("protocol", "http");
        protocol = commonParams.checked(protocol, "protocol", "https?");
        String urlIndex = commonParams.containIndex("url") ? "url" : null;
        return new QueryHash(qiniuConfig, algorithm, protocol, domain, urlIndex, savePath);
    }

    private ILineProcess<Map<String, String>> getStatFile() throws IOException {
        return new StatFile(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, savePath, saveFormat, saveSeparator);
    }

    private ILineProcess<Map<String, String>> getPrivateUrl() throws IOException {
        String domain = entryParam.getValue("domain", null);
        String protocol = entryParam.getValue("protocol", "http");
        protocol = commonParams.checked(protocol, "protocol", "https?");
        String urlIndex = commonParams.containIndex("url") ? "url" : null;
        String expires = entryParam.getValue("expires", "3600");
        expires = commonParams.checked(expires, "expires", "[1-9]\\d*");
        return new PrivateUrl(qiniuAccessKey, qiniuSecretKey, domain, protocol, urlIndex, Long.valueOf(expires), savePath);
    }

    private ILineProcess<Map<String, String>> getMirrorFile() throws IOException {
        return new MirrorFile(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, savePath);
    }

    private ILineProcess<Map<String, String>> getExportTs() throws IOException {
        String domain = entryParam.getValue("domain", null);
        String protocol = entryParam.getValue("protocol", "http");
        protocol = commonParams.checked(protocol, "protocol", "https?");
        String urlIndex = commonParams.containIndex("url") ? "url" : null;
        return new ExportTS(qiniuConfig, domain, protocol, urlIndex, savePath);
    }
}
