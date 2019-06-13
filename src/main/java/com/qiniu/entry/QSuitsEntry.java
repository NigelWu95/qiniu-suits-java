package com.qiniu.entry;

import com.aliyun.oss.ClientConfiguration;
import com.google.gson.JsonObject;
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
import com.qiniu.process.other.ExportTS;
import com.qiniu.process.qdora.*;
import com.qiniu.process.qoss.*;
import com.qiniu.sdk.UpYunConfig;
import com.qiniu.storage.Configuration;
import com.qiniu.util.OssUtils;
import com.qiniu.util.ParamsUtils;
import com.qiniu.util.ProcessUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class QSuitsEntry {

    private IEntryParam entryParam;
    private CommonParams commonParams;
    private int connectTimeout;
    private int readTimeout;
    private int requestTimeout;
    private Configuration qiniuConfig;
    private ClientConfig tenClientConfig;
    private ClientConfiguration aliClientConfig;
    private UpYunConfig upYunConfig;
    private String source;
    private String qiniuAccessKey;
    private String qiniuSecretKey;
    private String regionName;
    private String bucket;
    private Map<String, String> indexMap;
    private int unitLen;
    private int threads;
    private boolean saveTotal;
    private String process;
    private int retryTimes;
    private String savePath;
    private String saveFormat;
    private String saveSeparator;
    private Set<String> rmFields;

    public QSuitsEntry(String[] args) throws Exception {
        this.entryParam = new ParamsConfig(getEntryParams(args));
        this.commonParams = new CommonParams(entryParam);
        setMembers();
    }

    public QSuitsEntry(Map<String, String> paramsMap) throws Exception {
        this.entryParam = new ParamsConfig(paramsMap);
        this.commonParams = new CommonParams(paramsMap);
        setMembers();
    }

    public QSuitsEntry(IEntryParam entryParam) throws Exception {
        this.entryParam = entryParam;
        this.commonParams = new CommonParams(entryParam);
        setMembers();
    }

    public void updateEntry(IEntryParam entryParam) throws Exception {
        this.entryParam = entryParam;
        this.commonParams = new CommonParams(entryParam);
        setMembers();
    }

    public void updateEntry(CommonParams commonParams) {
        this.commonParams = commonParams;
        setMembers();
    }

    public void setQiniuConfig(Configuration configuration) throws IOException {
        if (configuration == null) throw new IOException("the configuration can not be null when you set it.");
        this.qiniuConfig = configuration;
    }

    public void setTenClientConfig(ClientConfig clientConfig) throws IOException {
        if (clientConfig == null) throw new IOException("the clientConfig can not be null when you set it.");
        this.tenClientConfig = clientConfig;
    }

    public void setAliClientConfig(ClientConfiguration clientConfig) throws IOException {
        if (clientConfig == null) throw new IOException("the clientConfiguration can not be null when you set it.");
        this.aliClientConfig = clientConfig;
    }

    public void setUpYunConfig(UpYunConfig upYunConfig) throws IOException {
        if (upYunConfig == null) throw new IOException("the clientConfiguration can not be null when you set it.");
        this.upYunConfig = upYunConfig;
    }

    private void setMembers() {
        this.connectTimeout = commonParams.getConnectTimeout();
        this.readTimeout = commonParams.getReadTimeout();
        this.requestTimeout = commonParams.getRequestTimeout();
        this.source = commonParams.getSource();
        this.qiniuAccessKey = commonParams.getQiniuAccessKey();
        this.qiniuSecretKey = commonParams.getQiniuSecretKey();
        this.bucket = commonParams.getBucket();
        this.regionName = commonParams.getRegionName();
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

    public static Map<String, String> getEntryParams(String[] args) throws IOException {
        Map<String, String> paramsMap;
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
                paramsMap = ParamsUtils.toParamsMap(new PropertiesFile(configFilePath).getProperties());
            } else {
                paramsMap = ParamsUtils.toParamsMap(configFilePath);
            }
        } else {
            paramsMap = ParamsUtils.toParamsMap(args);
            paramsMap.putAll(ParamsUtils.toParamsMap(args));
        }
        return paramsMap;
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
        Zone zone = OssUtils.getQiniuRegion(regionName);
        Configuration configuration = new Configuration(zone);
        if (connectTimeout > Constants.CONNECT_TIMEOUT) configuration.connectTimeout = connectTimeout;
        if (readTimeout> Constants.READ_TIMEOUT) configuration.readTimeout = readTimeout;
        if (requestTimeout > Constants.WRITE_TIMEOUT) configuration.writeTimeout = requestTimeout;
        return configuration;
    }

    public ClientConfig getTenClientConfig() throws IOException {
        return tenClientConfig == null ? getDefaultTenClientConfig() : tenClientConfig;
    }

    private ClientConfig getDefaultTenClientConfig() throws IOException {
        if (regionName == null || "".equals(regionName)) regionName = OssUtils.getTenCosRegion(
                commonParams.getTencentSecretId(), commonParams.getTencentSecretKey(), bucket);
        ClientConfig clientConfig = new ClientConfig(new Region(regionName));
        if (1000 * connectTimeout > clientConfig.getConnectionTimeout())
            clientConfig.setConnectionTimeout(1000 * connectTimeout);
        if (1000 * readTimeout > clientConfig.getSocketTimeout())
            clientConfig.setSocketTimeout(1000 * readTimeout);
        if (1000 * requestTimeout > clientConfig.getConnectionRequestTimeout())
            clientConfig.setConnectionRequestTimeout(1000 * requestTimeout);
        return clientConfig;
    }

    public ClientConfiguration getAliClientConfig() {
        return aliClientConfig == null ? getDefaultAliClientConfig() : aliClientConfig;
    }

    private ClientConfiguration getDefaultAliClientConfig() {
        ClientConfiguration clientConfig = new ClientConfiguration();
        if (1000 * connectTimeout > clientConfig.getConnectionTimeout())
            clientConfig.setConnectionTimeout(1000 * connectTimeout);
        if (1000 * readTimeout > clientConfig.getSocketTimeout())
            clientConfig.setSocketTimeout(1000 * readTimeout);
        if (1000 * requestTimeout > clientConfig.getConnectionRequestTimeout())
            clientConfig.setConnectionRequestTimeout(1000 * requestTimeout);
        return clientConfig;
    }

    public UpYunConfig getUpYunConfig() {
        return upYunConfig == null ? getDefaultUpYunConfig() : upYunConfig;
    }

    private UpYunConfig getDefaultUpYunConfig() {
        UpYunConfig upYunConfig = new UpYunConfig();
        if (1000 * connectTimeout > upYunConfig.connectTimeout)
            upYunConfig.connectTimeout = 1000 * connectTimeout;
        if (1000 * readTimeout > upYunConfig.readTimeout)
            upYunConfig.readTimeout = 1000 * readTimeout;
        return upYunConfig;
    }

    public IDataSource getDataSource() throws IOException {
        if ("qiniu".equals(source)) {
            return getQiniuOssContainer();
        } else if ("tencent".equals(source)) {
            return getTenOssContainer();
        } else if ("aliyun".equals(source)) {
            return getAliOssContainer();
        } else if ("upyun".equals(source)) {
            return getUpYunOssContainer();
        } else if ("local".equals(source)) {
            return getLocalFileContainer();
        } else {
            return null;
        }
    }

    public InputSource getScannerSource() {
        String parse = commonParams.getParse();
        String separator = commonParams.getSeparator();
        String addKeyPrefix = commonParams.getRmKeyPrefix();
        String rmKeyPrefix = commonParams.getRmKeyPrefix();
        return new InputSource(parse, separator, addKeyPrefix, rmKeyPrefix, indexMap);
    }

    public LocalFileContainer getLocalFileContainer() {
        String filePath = commonParams.getPath();
        String parse = commonParams.getParse();
        String separator = commonParams.getSeparator();
        String addKeyPrefix = commonParams.getRmKeyPrefix();
        String rmKeyPrefix = commonParams.getRmKeyPrefix();
        LocalFileContainer localFileContainer = new LocalFileContainer(filePath, parse, separator, addKeyPrefix,
                rmKeyPrefix, indexMap, unitLen, threads);
        localFileContainer.setSaveOptions(savePath, saveTotal, saveFormat, saveSeparator, rmFields);
        localFileContainer.setRetryTimes(retryTimes);
        return localFileContainer;
    }

    public QiniuOssContainer getQiniuOssContainer() {
        if (qiniuConfig == null) qiniuConfig = getDefaultQiniuConfig();
        Map<String, String[]> prefixesMap = commonParams.getPrefixesMap();
        List<String> antiPrefixes = commonParams.getAntiPrefixes();
        boolean prefixLeft = commonParams.getPrefixLeft();
        boolean prefixRight = commonParams.getPrefixRight();
        QiniuOssContainer qiniuOssContainer = new QiniuOssContainer(qiniuAccessKey, qiniuSecretKey, qiniuConfig,
                bucket, antiPrefixes, prefixesMap, prefixLeft, prefixRight, indexMap, unitLen, threads);
        qiniuOssContainer.setSaveOptions(savePath, saveTotal, saveFormat, saveSeparator, rmFields);
        qiniuOssContainer.setRetryTimes(retryTimes);
        return qiniuOssContainer;
    }

    public TenOssContainer getTenOssContainer() throws IOException {
        String secretId = commonParams.getTencentSecretId();
        String secretKey = commonParams.getTencentSecretKey();
        if (tenClientConfig == null) tenClientConfig = getDefaultTenClientConfig();
        Map<String, String[]> prefixesMap = commonParams.getPrefixesMap();
        List<String> antiPrefixes = commonParams.getAntiPrefixes();
        boolean prefixLeft = commonParams.getPrefixLeft();
        boolean prefixRight = commonParams.getPrefixRight();
        TenOssContainer tenOssContainer = new TenOssContainer(secretId, secretKey, tenClientConfig, bucket,
                antiPrefixes, prefixesMap, prefixLeft, prefixRight, indexMap, unitLen, threads);
        tenOssContainer.setSaveOptions(savePath, saveTotal, saveFormat, saveSeparator, rmFields);
        tenOssContainer.setRetryTimes(retryTimes);
        return tenOssContainer;
    }

    public AliOssContainer getAliOssContainer() throws IOException {
        String accessId = commonParams.getAliyunAccessId();
        String accessSecret = commonParams.getAliyunAccessSecret();
        String endPoint;
        if (regionName == null || "".equals(regionName)) regionName = OssUtils.getAliOssRegion(accessId, accessSecret, bucket);
        if (regionName.matches("https?://.+")) {
            endPoint = regionName;
        } else {
            if (!regionName.startsWith("oss-")) regionName = "oss-" + regionName;
            endPoint = "http://" + regionName + ".aliyuncs.com";
        }
        if (aliClientConfig == null) aliClientConfig = getDefaultAliClientConfig();
        Map<String, String[]> prefixesMap = commonParams.getPrefixesMap();
        List<String> antiPrefixes = commonParams.getAntiPrefixes();
        boolean prefixLeft = commonParams.getPrefixLeft();
        boolean prefixRight = commonParams.getPrefixRight();
        AliOssContainer aliOssContainer = new AliOssContainer(accessId, accessSecret, aliClientConfig, endPoint, bucket,
                antiPrefixes, prefixesMap, prefixLeft, prefixRight, indexMap, unitLen, threads);
        aliOssContainer.setSaveOptions(savePath, saveTotal, saveFormat, saveSeparator, rmFields);
        aliOssContainer.setRetryTimes(retryTimes);
        return aliOssContainer;
    }

    public UpYunOssContainer getUpYunOssContainer() {
        String username = commonParams.getUpyunUsername();
        String password = commonParams.getUpyunPassword();
        if (upYunConfig == null) upYunConfig = getDefaultUpYunConfig();
        Map<String, String[]> prefixesMap = commonParams.getPrefixesMap();
        List<String> antiPrefixes = commonParams.getAntiPrefixes();
        boolean prefixLeft = commonParams.getPrefixLeft();
        boolean prefixRight = commonParams.getPrefixRight();
        UpYunOssContainer upYunOssContainer = new UpYunOssContainer(username, password, upYunConfig, bucket,
                antiPrefixes, prefixesMap, prefixLeft, prefixRight, indexMap, unitLen, threads);
        upYunOssContainer.setSaveOptions(savePath, saveTotal, saveFormat, saveSeparator, rmFields);
        upYunOssContainer.setRetryTimes(retryTimes);
        return upYunOssContainer;
    }

    public ILineProcess<Map<String, String>> getProcessor() throws Exception {
        ILineProcess<Map<String, String>> nextProcessor = process == null ? null : whichNextProcessor(false);
        ILineProcess<Map<String, String>> processor;
        BaseFilter<Map<String, String>> baseFilter = commonParams.getBaseFilter();
        SeniorFilter<Map<String, String>> seniorFilter = commonParams.getSeniorFilter();
        if (baseFilter != null || seniorFilter != null) {
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

    public ILineProcess<Map<String, String>> whichNextProcessor(boolean single) throws Exception {
        ILineProcess<Map<String, String>> processor = null;
        if (qiniuConfig == null) qiniuConfig = getDefaultQiniuConfig();
        switch (process) {
            case "status": processor = getChangeStatus(single); break;
            case "type": processor = getChangeType(single); break;
            case "lifecycle": processor = getChangeLifecycle(single); break;
            case "copy": processor = getCopyFile(single); break;
            case "move":
            case "rename": processor = getMoveFile(single); break;
            case "delete": processor = getDeleteFile(single); break;
            case "asyncfetch": processor = getAsyncFetch(single); break;
            case "avinfo": processor = getQueryAvinfo(single); break;
            case "pfopcmd": processor = getPfopCommand(single); break;
            case "pfop": processor = getPfop(single); break;
            case "pfopresult": processor = getPfopResult(single); break;
            case "qhash": processor = getQueryHash(single); break;
            case "stat": processor = getStatFile(single); break;
            case "privateurl": processor = getPrivateUrl(single); break;
            case "mirror": processor = getMirrorFile(single); break;
            case "exportts": processor = getExportTs(single); break;
        }
        if (processor != null) {
            if (ProcessUtils.canBatch(processor.getProcessName())) processor.setBatchSize(commonParams.getBatchSize());
            // 为了保证程序出现因网络等原因产生的非预期异常时正常运行需要设置重试次数
            processor.setRetryTimes(retryTimes);
        }
        return processor;
    }

    private ILineProcess<Map<String, String>> getChangeStatus(boolean single) throws IOException {
        String status = ParamsUtils.checked(entryParam.getValue("status"), "status", "[01]");
        return single ? new ChangeStatus(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, Integer.valueOf(status))
                : new ChangeStatus(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, Integer.valueOf(status), savePath);
    }

    private ILineProcess<Map<String, String>> getChangeType(boolean single) throws IOException {
        String type = ParamsUtils.checked(entryParam.getValue("type"), "type", "[01]");
        return single ? new ChangeType(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, Integer.valueOf(type))
                : new ChangeType(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, Integer.valueOf(type), savePath);
    }

    private ILineProcess<Map<String, String>> getChangeLifecycle(boolean single) throws IOException {
        String days = ParamsUtils.checked(entryParam.getValue("days"), "days", "\\d");
        return single ? new ChangeLifecycle(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, Integer.valueOf(days))
                : new ChangeLifecycle(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, Integer.valueOf(days), savePath);
    }

    private ILineProcess<Map<String, String>> getCopyFile(boolean single) throws IOException {
        String toBucket = entryParam.getValue("to-bucket");
        String toKeyIndex = indexMap.containsValue("toKey") ? "toKey" : null;
        String addPrefix = entryParam.getValue("add-prefix", null);
        String rmPrefix = entryParam.getValue("rm-prefix", null);
        return single ? new CopyFile(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, toBucket, toKeyIndex, addPrefix,
                rmPrefix)
                : new CopyFile(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, toBucket, toKeyIndex, addPrefix,
                rmPrefix, savePath);
    }

    private ILineProcess<Map<String, String>> getMoveFile(boolean single) throws IOException {
        String toBucket = entryParam.getValue("to-bucket", null);
        if ("move".equals(process) && toBucket == null) throw new IOException("no incorrect to-bucket, please set it.");
        String toKeyIndex = indexMap.containsValue("toKey") ? "toKey" : null;
        String addPrefix = entryParam.getValue("add-prefix", null);
        String force = entryParam.getValue("prefix-force", "false");
        force = ParamsUtils.checked(force, "prefix-force", "(true|false)");
        String rmPrefix = entryParam.getValue("rm-prefix", null);
        return single ? new MoveFile(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, toBucket, toKeyIndex, addPrefix,
                Boolean.valueOf(force), rmPrefix)
                : new MoveFile(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, toBucket, toKeyIndex, addPrefix,
                Boolean.valueOf(force), rmPrefix, savePath);
    }

    private ILineProcess<Map<String, String>> getDeleteFile(boolean single) throws IOException {
        return single ? new DeleteFile(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket)
                : new DeleteFile(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, savePath);
    }

    private ILineProcess<Map<String, String>> getAsyncFetch(boolean single) throws IOException {
        String toBucket = entryParam.getValue("to-bucket");
        String domain = entryParam.getValue("domain", null);
        String protocol = entryParam.getValue("protocol", "http");
        protocol = ParamsUtils.checked(protocol, "protocol", "https?");
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        String addPrefix = entryParam.getValue("add-prefix", null);
        String rmPrefix = entryParam.getValue("rm-prefix", null);
        String host = entryParam.getValue("host", null);
        String md5Index = indexMap.containsValue("md5") ? "md5" : null;
        String callbackUrl = entryParam.getValue("callback-url", null);
        String callbackBody = entryParam.getValue("callback-body", null);
        String callbackBodyType = entryParam.getValue("callback-body-type", null);
        String callbackHost = entryParam.getValue("callback-host", null);
        String type = entryParam.getValue("file-type", "0");
        String ignore = entryParam.getValue("ignore-same-key", "false");
        ignore = ParamsUtils.checked(ignore, "ignore-same-key", "(true|false)");
        ILineProcess<Map<String, String>> processor = single ? new AsyncFetch(qiniuAccessKey, qiniuSecretKey, qiniuConfig,
                toBucket, domain, protocol, urlIndex, addPrefix, rmPrefix)
                : new AsyncFetch(qiniuAccessKey, qiniuSecretKey, qiniuConfig, toBucket, domain, protocol, urlIndex,
                addPrefix, rmPrefix, savePath);
        if (host != null || md5Index != null || callbackUrl != null || callbackBody != null || callbackBodyType != null
                || callbackHost != null || "1".equals(type) || "true".equals(ignore)) {
            ((AsyncFetch) processor).setFetchArgs(host, md5Index, callbackUrl, callbackBody,
                    callbackBodyType, callbackHost, Integer.valueOf(type), Boolean.valueOf(ignore));
        }
        return processor;
    }

    private ILineProcess<Map<String, String>> getQueryAvinfo(boolean single) throws IOException {
        String domain = entryParam.getValue("domain", null);
        String protocol = entryParam.getValue("protocol", "http");
        protocol = ParamsUtils.checked(protocol, "protocol", "https?");
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        return single ? new QueryAvinfo(qiniuConfig, domain, protocol, urlIndex)
                : new QueryAvinfo(qiniuConfig, domain, protocol, urlIndex, savePath);
    }

    private ILineProcess<Map<String, String>> getPfopCommand(boolean single) throws IOException {
        String avinfoIndex = indexMap.containsValue("avinfo") ? "avinfo" : null;
        String duration = entryParam.getValue("duration", "false");
        duration = ParamsUtils.checked(duration, "duration", "(true|false)");
        String size = entryParam.getValue("size", "false");
        size = ParamsUtils.checked(size, "size", "(true|false)");
        String configJson = entryParam.getValue("pfop-config", null);
        List<JsonObject> pfopConfigs = commonParams.getPfopConfigs();
        return single ? new PfopCommand(qiniuConfig, avinfoIndex, Boolean.valueOf(duration), Boolean.valueOf(size),
                configJson, pfopConfigs)
                : new PfopCommand(qiniuConfig, avinfoIndex, Boolean.valueOf(duration), Boolean.valueOf(size), configJson,
                pfopConfigs, savePath);
    }

    private ILineProcess<Map<String, String>> getPfop(boolean single) throws IOException {
        String pipeline = entryParam.getValue("pipeline", null);
        String forcePublic = entryParam.getValue("force-public", "false");
        if (pipeline == null && !"true".equals(forcePublic)) {
            throw new IOException("please set pipeline, if you don't want to use" +
                    " private pipeline, please set the force-public as true.");
        }
        String configJson = entryParam.getValue("pfop-config", null);
        List<JsonObject> pfopConfigs = commonParams.getPfopConfigs();
        String fopsIndex = indexMap.containsValue("fops") ? "fops" : null;
        return single ? new QiniuPfop(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, pipeline, configJson,
                pfopConfigs, fopsIndex)
                : new QiniuPfop(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, pipeline, configJson, pfopConfigs,
                fopsIndex, savePath);
    }

    private ILineProcess<Map<String, String>> getPfopResult(boolean single) throws IOException {
        String protocol = entryParam.getValue("protocol", "http");
        protocol = ParamsUtils.checked(protocol, "protocol", "https?");
        String persistentIdIndex = indexMap.containsValue("pid") ? "pid" : null;
        return single ? new QueryPfopResult(qiniuConfig, protocol, persistentIdIndex)
                : new QueryPfopResult(qiniuConfig, protocol, persistentIdIndex, savePath);
    }

    private ILineProcess<Map<String, String>> getQueryHash(boolean single) throws IOException {
        String domain = entryParam.getValue("domain", null);
        String algorithm = entryParam.getValue("algorithm", "md5");
        algorithm = ParamsUtils.checked(algorithm, "algorithm", "(md5|sha1)");
        String protocol = entryParam.getValue("protocol", "http");
        protocol = ParamsUtils.checked(protocol, "protocol", "https?");
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        return single ? new QueryHash(qiniuConfig, algorithm, protocol, domain, urlIndex)
                : new QueryHash(qiniuConfig, algorithm, protocol, domain, urlIndex, savePath);
    }

    private ILineProcess<Map<String, String>> getStatFile(boolean single) throws IOException {
        return single ? new StatFile(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, saveFormat, saveSeparator)
                : new StatFile(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, savePath, saveFormat, saveSeparator);
    }

    private ILineProcess<Map<String, String>> getPrivateUrl(boolean single) throws IOException {
        String domain = entryParam.getValue("domain", null);
        String protocol = entryParam.getValue("protocol", "http");
        protocol = ParamsUtils.checked(protocol, "protocol", "https?");
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        String expires = entryParam.getValue("expires", "3600");
        expires = ParamsUtils.checked(expires, "expires", "[1-9]\\d*");
        return single ? new PrivateUrl(qiniuAccessKey, qiniuSecretKey, domain, protocol, urlIndex, Long.valueOf(expires))
                : new PrivateUrl(qiniuAccessKey, qiniuSecretKey, domain, protocol, urlIndex, Long.valueOf(expires), savePath);
    }

    private ILineProcess<Map<String, String>> getMirrorFile(boolean single) throws IOException {
        return single ? new MirrorFile(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket)
                : new MirrorFile(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket, savePath);
    }

    private ILineProcess<Map<String, String>> getExportTs(boolean single) throws IOException {
        String domain = entryParam.getValue("domain", null);
        String protocol = entryParam.getValue("protocol", "http");
        protocol = ParamsUtils.checked(protocol, "protocol", "https?");
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        return single ? new ExportTS(qiniuConfig, domain, protocol, urlIndex)
                : new ExportTS(qiniuConfig, domain, protocol, urlIndex, savePath);
    }
}
