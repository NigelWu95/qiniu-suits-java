package com.qiniu.entry;

import com.aliyun.oss.ClientConfiguration;
import com.google.gson.JsonObject;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.region.Region;
import com.qiniu.common.Constants;
import com.qiniu.common.SuitsException;
import com.qiniu.common.Zone;
import com.qiniu.convert.MapToString;
import com.qiniu.datasource.*;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.process.filtration.*;
import com.qiniu.process.other.DownloadFile;
import com.qiniu.process.other.ExportTS;
import com.qiniu.process.qai.*;
import com.qiniu.process.qdora.*;
import com.qiniu.process.qos.*;
import com.qiniu.sdk.UpYunConfig;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

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
    private com.amazonaws.ClientConfiguration s3ClientConfig;
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
    private List<String> rmFields;

    public QSuitsEntry(IEntryParam entryParam) throws Exception {
        this.entryParam = entryParam;
        this.commonParams = new CommonParams(entryParam);
        setMembers();
    }

    public QSuitsEntry(IEntryParam entryParam, CommonParams commonParams) {
        this.entryParam = entryParam;
        this.commonParams = commonParams;
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
        this.savePath = commonParams.getSavePath();
        this.saveFormat = commonParams.getSaveFormat();
        this.saveSeparator = commonParams.getSaveSeparator();
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
        Zone zone = CloudAPIUtils.getQiniuRegion(regionName);
        Configuration configuration = new Configuration(zone);
        if (connectTimeout > Constants.CONNECT_TIMEOUT) configuration.connectTimeout = connectTimeout;
        if (readTimeout> Constants.READ_TIMEOUT) configuration.readTimeout = readTimeout;
        if (requestTimeout > Constants.WRITE_TIMEOUT) configuration.writeTimeout = requestTimeout;
        return configuration;
    }

    public ClientConfig getTenClientConfig() throws IOException {
        return tenClientConfig == null ? getDefaultTenClientConfig() : tenClientConfig;
    }

    private ClientConfig getDefaultTenClientConfig() throws SuitsException {
        if (regionName == null || "".equals(regionName)) regionName = CloudAPIUtils.getTenCosRegion(
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

    public com.amazonaws.ClientConfiguration getS3ClientConfig() {
        return s3ClientConfig == null ? getDefaultS3ClientConfig() : s3ClientConfig;
    }

    private com.amazonaws.ClientConfiguration getDefaultS3ClientConfig() {
        com.amazonaws.ClientConfiguration clientConfig = new com.amazonaws.ClientConfiguration();
        if (1000 * connectTimeout > clientConfig.getConnectionTimeout())
            clientConfig.setConnectionTimeout(1000 * connectTimeout);
        if (1000 * readTimeout > clientConfig.getSocketTimeout())
            clientConfig.setSocketTimeout(1000 * readTimeout);
        if (1000 * requestTimeout > clientConfig.getRequestTimeout())
            clientConfig.setRequestTimeout(1000 * requestTimeout);
        return clientConfig;
    }

    public IDataSource getDataSource() throws IOException {
        if ("qiniu".equals(source)) {
            return getQiniuQosContainer();
        } else if ("tencent".equals(source)) {
            return getTenCosContainer();
        } else if ("aliyun".equals(source)) {
            return getAliOssContainer();
        } else if ("upyun".equals(source)) {
            return getUpYosContainer();
        } else if ("s3".equals(source)) {
            return getAwsS3Container();
        } else if ("local".equals(source)) {
            return getLocalFileContainer();
        } else {
            return null;
        }
    }

    public InputSource getInputSource() {
        String parse = commonParams.getParse();
        String separator = commonParams.getSeparator();
        String addKeyPrefix = commonParams.getRmKeyPrefix();
        String rmKeyPrefix = commonParams.getRmKeyPrefix();
        return new InputSource(parse, separator, addKeyPrefix, rmKeyPrefix, indexMap);
    }

    public LocalFileContainer getLocalFileContainer() throws IOException {
        String filePath = commonParams.getPath();
        String parse = commonParams.getParse();
        String separator = commonParams.getSeparator();
        String addKeyPrefix = commonParams.getAddKeyPrefix();
        String rmKeyPrefix = commonParams.getRmKeyPrefix();
        LocalFileContainer localFileContainer = new LocalFileContainer(filePath, parse, separator, addKeyPrefix,
                rmKeyPrefix, indexMap, commonParams.getToStringFields(), unitLen, threads);
        localFileContainer.setSaveOptions(saveTotal, savePath, saveFormat, saveSeparator, rmFields);
        localFileContainer.setRetryTimes(retryTimes);
        return localFileContainer;
    }

    public QiniuQosContainer getQiniuQosContainer() throws IOException {
        if (qiniuConfig == null) qiniuConfig = getDefaultQiniuConfig();
        Map<String, Map<String, String>> prefixesMap = commonParams.getPrefixesMap();
        List<String> antiPrefixes = commonParams.getAntiPrefixes();
        boolean prefixLeft = commonParams.getPrefixLeft();
        boolean prefixRight = commonParams.getPrefixRight();
        QiniuQosContainer qiniuQosContainer = new QiniuQosContainer(qiniuAccessKey, qiniuSecretKey, qiniuConfig,
                bucket, antiPrefixes, prefixesMap, prefixLeft, prefixRight, indexMap, commonParams.getToStringFields(),
                unitLen, threads);
        qiniuQosContainer.setSaveOptions(saveTotal, savePath, saveFormat, saveSeparator, rmFields);
        qiniuQosContainer.setRetryTimes(retryTimes);
        return qiniuQosContainer;
    }

    public TenCosContainer getTenCosContainer() throws IOException {
        String secretId = commonParams.getTencentSecretId();
        String secretKey = commonParams.getTencentSecretKey();
        if (tenClientConfig == null) tenClientConfig = getDefaultTenClientConfig();
        Map<String, Map<String, String>> prefixesMap = commonParams.getPrefixesMap();
        List<String> antiPrefixes = commonParams.getAntiPrefixes();
        boolean prefixLeft = commonParams.getPrefixLeft();
        boolean prefixRight = commonParams.getPrefixRight();
        TenCosContainer tenCosContainer = new TenCosContainer(secretId, secretKey, tenClientConfig, bucket,
                antiPrefixes, prefixesMap, prefixLeft, prefixRight, indexMap, commonParams.getToStringFields(), unitLen, threads);
        tenCosContainer.setSaveOptions(saveTotal, savePath, saveFormat, saveSeparator, rmFields);
        tenCosContainer.setRetryTimes(retryTimes);
        return tenCosContainer;
    }

    public AliOssContainer getAliOssContainer() throws IOException {
        String accessId = commonParams.getAliyunAccessId();
        String accessSecret = commonParams.getAliyunAccessSecret();
        String endPoint;
        if (regionName == null || "".equals(regionName)) regionName = CloudAPIUtils.getAliOssRegion(accessId, accessSecret, bucket);
        if (regionName.matches("https?://.+")) {
            endPoint = regionName;
        } else {
            if (!regionName.startsWith("oss-")) regionName = "oss-" + regionName;
            endPoint = "http://" + regionName + ".aliyuncs.com";
        }
        if (aliClientConfig == null) aliClientConfig = getDefaultAliClientConfig();
        Map<String, Map<String, String>> prefixesMap = commonParams.getPrefixesMap();
        List<String> antiPrefixes = commonParams.getAntiPrefixes();
        boolean prefixLeft = commonParams.getPrefixLeft();
        boolean prefixRight = commonParams.getPrefixRight();
        AliOssContainer aliOssContainer = new AliOssContainer(accessId, accessSecret, aliClientConfig, endPoint, bucket,
                antiPrefixes, prefixesMap, prefixLeft, prefixRight, indexMap, commonParams.getToStringFields(), unitLen, threads);
        aliOssContainer.setSaveOptions(saveTotal, savePath, saveFormat, saveSeparator, rmFields);
        aliOssContainer.setRetryTimes(retryTimes);
        return aliOssContainer;
    }

    public UpYosContainer getUpYosContainer() throws IOException {
        String username = commonParams.getUpyunUsername();
        String password = commonParams.getUpyunPassword();
        if (upYunConfig == null) upYunConfig = getDefaultUpYunConfig();
        Map<String, Map<String, String>> prefixesMap = commonParams.getPrefixesMap();
        List<String> antiPrefixes = commonParams.getAntiPrefixes();
//        boolean prefixLeft = commonParams.getPrefixLeft();
//        boolean prefixRight = commonParams.getPrefixRight();
        UpYosContainer upYosContainer = new UpYosContainer(username, password, upYunConfig, bucket, antiPrefixes, prefixesMap,
//                prefixLeft, prefixRight,
                indexMap, commonParams.getToStringFields(), unitLen, threads);
        upYosContainer.setSaveOptions(saveTotal, savePath, saveFormat, saveSeparator, rmFields);
        upYosContainer.setRetryTimes(retryTimes);
        return upYosContainer;
    }

    public AwsS3Container getAwsS3Container() throws IOException {
        String s3AccessId = commonParams.getS3AccessId();
        String s3SecretKey = commonParams.getS3SecretKey();
        if (s3ClientConfig == null) s3ClientConfig = getDefaultS3ClientConfig();
        Map<String, Map<String, String>> prefixesMap = commonParams.getPrefixesMap();
        List<String> antiPrefixes = commonParams.getAntiPrefixes();
        boolean prefixLeft = commonParams.getPrefixLeft();
        boolean prefixRight = commonParams.getPrefixRight();
        if (regionName == null || "".equals(regionName)) regionName = CloudAPIUtils.getS3Region(s3AccessId, s3SecretKey, bucket);
        AwsS3Container awsS3Container = new AwsS3Container(s3AccessId, s3SecretKey, s3ClientConfig, regionName, bucket,
                antiPrefixes, prefixesMap, prefixLeft, prefixRight, indexMap, commonParams.getToStringFields(), unitLen, threads);
        awsS3Container.setSaveOptions(saveTotal, savePath,  saveFormat, saveSeparator, rmFields);
        awsS3Container.setRetryTimes(retryTimes);
        return awsS3Container;
    }

    public ILineProcess<Map<String, String>> getProcessor() throws Exception {
        ILineProcess<Map<String, String>> nextProcessor = process == null ? null : whichNextProcessor(false);
        BaseFilter<Map<String, String>> baseFilter = commonParams.getBaseFilter();
        SeniorFilter<Map<String, String>> seniorFilter = commonParams.getSeniorFilter();
        ILineProcess<Map<String, String>> processor;
        if (baseFilter != null || seniorFilter != null) {
            List<String> fields = commonParams.getToStringFields();
            if (fields == null || fields.size() == 0) fields = ConvertingUtils.getFields(new ArrayList<>(indexMap.values()), rmFields);
            if (nextProcessor == null) {
                List<String> finalFields = fields;
                processor = new FilterProcess<Map<String, String>>(baseFilter, seniorFilter, savePath, saveFormat,
                        saveSeparator, rmFields) {
                    @Override
                    protected ITypeConvert<Map<String, String>, String> newTypeConverter() throws IOException {
                        return new MapToString(saveFormat, saveSeparator, finalFields);
                    }
                };
            } else {
                processor = new FilterProcess<Map<String, String>>(baseFilter, seniorFilter){};
                processor.setNextProcessor(nextProcessor);
            }
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
            case "copy": processor = getCopyFile(indexMap, single); break;
            case "move":
            case "rename": processor = getMoveFile(indexMap, single); break;
            case "delete": processor = getDeleteFile(single); break;
            case "asyncfetch":
                processor = getPrivateTypeProcessor(single);
                if (processor != null) {
                    ILineProcess<Map<String, String>> fetchProcessor = getAsyncFetch(new HashMap<String, String>(){{
                        putAll(indexMap);
                        put("url", "url");
                    }}, true);
                    fetchProcessor.setRetryTimes(retryTimes);
                    processor.setNextProcessor(fetchProcessor);
                } else {
                    processor = getAsyncFetch(indexMap, single);
                }
                break;
            case "avinfo": processor = getQueryAvinfo(indexMap, single); break;
            case "pfopcmd": processor = getPfopCommand(indexMap, single); break;
            case "pfop": processor = getPfop(indexMap, single); break;
            case "pfopresult": processor = getPfopResult(indexMap, single); break;
            case "qhash": processor = getQueryHash(indexMap, single); break;
            case "stat": processor = getStatFile(single); break;
            case "privateurl": processor = getPrivateUrl(indexMap, single); break;
            case "mirror": processor = getMirrorFile(single); break;
            case "exportts": processor = getExportTs(indexMap, single); break;
            case "tenprivate": processor = getTencentPrivateUrl(single); break;
            case "s3private": case "awsprivate": processor = getAwsS3PrivateUrl(single); break;
            case "aliprivate": processor = getAliyunPrivateUrl(single); break;
            case "download":
                processor = getPrivateTypeProcessor(single);
                if (processor != null) {
                    ILineProcess<Map<String, String>> downProcessor = getDownloadFile(new HashMap<String, String>(){{
                        putAll(indexMap);
                        put("url", "url");
                    }}, true);
                    downProcessor.setRetryTimes(retryTimes);
                    processor.setNextProcessor(downProcessor);
                } else {
                    processor = getDownloadFile(indexMap, single);
                }
                break;
            case "filter": case "": break;
            default: throw new IOException("unsupported process: " + process);
        }
        if (processor != null) {
            if (ProcessUtils.canBatch(processor.getProcessName())) processor.setBatchSize(commonParams.getBatchSize());
            // 为了保证程序出现因网络等原因产生的非预期异常时正常运行需要设置重试次数
            processor.setRetryTimes(retryTimes);
        }
        return processor;
    }

    private ILineProcess<Map<String, String>> getPrivateTypeProcessor(boolean single) throws IOException {
        ILineProcess<Map<String, String>> processor = null;
        String privateType = commonParams.getPrivateType();
        if ("qiniu".equals(privateType)) {
            processor = getPrivateUrl(indexMap, single);
        } else if ("tencent".equals(privateType)) {
            processor = getTencentPrivateUrl(single);
        } else if ("aliyun".equals(privateType)) {
            processor = getAliyunPrivateUrl(single);
        } else if ("s3".equals(privateType) || "aws".equals(privateType)) {
            processor = getAwsS3PrivateUrl(single);
        } else if (privateType != null && !"".equals(privateType)) {
            throw new IOException("unsupported private process: " + privateType + " for asyncfetch's url.");
        }
        return processor;
    }

    private ILineProcess<Map<String, String>> getChangeStatus(boolean single) throws IOException {
        String status = ParamsUtils.checked(entryParam.getValue("status").trim(), "status", "[01]");
        return single ? new ChangeStatus(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, Integer.valueOf(status))
                : new ChangeStatus(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, Integer.valueOf(status), savePath);
    }

    private ILineProcess<Map<String, String>> getChangeType(boolean single) throws IOException {
        String type = ParamsUtils.checked(entryParam.getValue("type").trim(), "type", "[01]");
        return single ? new ChangeType(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, Integer.valueOf(type))
                : new ChangeType(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, Integer.valueOf(type), savePath);
    }

    private ILineProcess<Map<String, String>> getChangeLifecycle(boolean single) throws IOException {
        String days = ParamsUtils.checked(entryParam.getValue("days").trim(), "days", "\\d+");
        return single ? new ChangeLifecycle(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, Integer.valueOf(days))
                : new ChangeLifecycle(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, Integer.valueOf(days), savePath);
    }

    private ILineProcess<Map<String, String>> getCopyFile(Map<String, String> indexMap, boolean single) throws IOException {
        String toBucket = entryParam.getValue("to-bucket").trim();
        String toKeyIndex = indexMap.containsValue("toKey") ? "toKey" : null;
        String addPrefix = entryParam.getValue("add-prefix", null);
        String rmPrefix = entryParam.getValue("rm-prefix", null);
        return single ? new CopyFile(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, toBucket, toKeyIndex, addPrefix,
                rmPrefix)
                : new CopyFile(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, toBucket, toKeyIndex, addPrefix,
                rmPrefix, savePath);
    }

    private ILineProcess<Map<String, String>> getMoveFile(Map<String, String> indexMap, boolean single) throws IOException {
        String toBucket = entryParam.getValue("to-bucket", "").trim();
        if ("move".equals(process)) {
            if (toBucket.isEmpty()) throw new IOException("no incorrect to-bucket, please set it.");
        } else {
            toBucket = null;
        }
        String toKeyIndex = indexMap.containsValue("toKey") ? "toKey" : null;
        String addPrefix = entryParam.getValue("add-prefix", null);
        String force = entryParam.getValue("prefix-force", "false").trim();
        force = ParamsUtils.checked(force, "prefix-force", "(true|false)");
        String rmPrefix = entryParam.getValue("rm-prefix", null);
        return single ? new MoveFile(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, toBucket, toKeyIndex, addPrefix,
                Boolean.valueOf(force), rmPrefix)
                : new MoveFile(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, toBucket, toKeyIndex, addPrefix,
                Boolean.valueOf(force), rmPrefix, savePath);
    }

    private ILineProcess<Map<String, String>> getDeleteFile(boolean single) throws IOException {
        return single ? new DeleteFile(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket)
                : new DeleteFile(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, savePath);
    }

    private ILineProcess<Map<String, String>> getAsyncFetch(Map<String, String> indexMap, boolean single) throws IOException {
        String toBucket = entryParam.getValue("to-bucket").trim();
        String domain = entryParam.getValue("domain", "").trim();
        String protocol = entryParam.getValue("protocol", "http").trim();
        protocol = ParamsUtils.checked(protocol, "protocol", "https?");
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        String addPrefix = entryParam.getValue("add-prefix", null);
        String rmPrefix = entryParam.getValue("rm-prefix", null);
        String host = entryParam.getValue("host", "").trim();
        String md5Index = indexMap.containsValue("md5") ? "md5" : null;
        String callbackUrl = entryParam.getValue("callback-url", "").trim();
        String callbackBody = entryParam.getValue("callback-body", "").trim();
        String callbackBodyType = entryParam.getValue("callback-body-type", "").trim();
        String callbackHost = entryParam.getValue("callback-host", "").trim();
        String type = entryParam.getValue("file-type", "0").trim();
        String ignore = entryParam.getValue("ignore-same-key", "false").trim();
        ignore = ParamsUtils.checked(ignore, "ignore-same-key", "(true|false)");
        ILineProcess<Map<String, String>> processor = single ? new AsyncFetch(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(),
                toBucket, domain, protocol, urlIndex, addPrefix, rmPrefix)
                : new AsyncFetch(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), toBucket, domain, protocol, urlIndex,
                addPrefix, rmPrefix, savePath);
        if (!host.isEmpty() || md5Index != null || !callbackUrl.isEmpty() || !callbackBody.isEmpty() ||
                !callbackBodyType.isEmpty() || !callbackHost.isEmpty() || "1".equals(type) || "true".equals(ignore)) {
            ((AsyncFetch) processor).setFetchArgs(host, md5Index, callbackUrl, callbackBody,
                    callbackBodyType, callbackHost, Integer.valueOf(type), Boolean.valueOf(ignore));
        }
        return processor;
    }

    private ILineProcess<Map<String, String>> getQueryAvinfo(Map<String, String> indexMap, boolean single) throws IOException {
        String domain = entryParam.getValue("domain", "").trim();
        String protocol = entryParam.getValue("protocol", "http").trim();
        protocol = ParamsUtils.checked(protocol, "protocol", "https?");
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        return single ? new QueryAvinfo(getQiniuConfig(), domain, protocol, urlIndex)
                : new QueryAvinfo(getQiniuConfig(), domain, protocol, urlIndex, savePath);
    }

    private ILineProcess<Map<String, String>> getPfopCommand(Map<String, String> indexMap, boolean single) throws IOException {
        String avinfoIndex = indexMap.containsValue("avinfo") ? "avinfo" : null;
        String duration = entryParam.getValue("duration", "false").trim();
        duration = ParamsUtils.checked(duration, "duration", "(true|false)");
        String size = entryParam.getValue("size", "false").trim();
        size = ParamsUtils.checked(size, "size", "(true|false)");
        String configJson = entryParam.getValue("pfop-config", "").trim();
        List<JsonObject> pfopConfigs = commonParams.getPfopConfigs();
        return single ? new PfopCommand(getQiniuConfig(), avinfoIndex, Boolean.valueOf(duration), Boolean.valueOf(size),
                configJson, pfopConfigs)
                : new PfopCommand(getQiniuConfig(), avinfoIndex, Boolean.valueOf(duration), Boolean.valueOf(size), configJson,
                pfopConfigs, savePath);
    }

    private ILineProcess<Map<String, String>> getPfop(Map<String, String> indexMap, boolean single) throws IOException {
        String pipeline = entryParam.getValue("pipeline", "").trim();
        String forcePublic = entryParam.getValue("force-public", "false").trim();
        if (pipeline.isEmpty() && !"true".equals(forcePublic)) {
            throw new IOException("please set pipeline, if you don't want to use" +
                    " private pipeline, please set the force-public as true.");
        }
        String configJson = entryParam.getValue("pfop-config", "").trim();
        List<JsonObject> pfopConfigs = commonParams.getPfopConfigs();
        String fopsIndex = indexMap.containsValue("fops") ? "fops" : null;
        return single ? new QiniuPfop(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, pipeline, configJson,
                pfopConfigs, fopsIndex)
                : new QiniuPfop(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, pipeline, configJson, pfopConfigs,
                fopsIndex, savePath);
    }

    private ILineProcess<Map<String, String>> getPfopResult(Map<String, String> indexMap, boolean single) throws IOException {
        String protocol = entryParam.getValue("protocol", "http").trim();
        protocol = ParamsUtils.checked(protocol, "protocol", "https?");
        String pIdIndex = indexMap.containsValue("id") ? "id" : null;
        return single ? new QueryPfopResult(getQiniuConfig(), protocol, pIdIndex)
                : new QueryPfopResult(getQiniuConfig(), protocol, pIdIndex, savePath);
    }

    private ILineProcess<Map<String, String>> getQueryHash(Map<String, String> indexMap, boolean single) throws IOException {
        String domain = entryParam.getValue("domain", "").trim();
        String algorithm = entryParam.getValue("algorithm", "md5").trim();
        algorithm = ParamsUtils.checked(algorithm, "algorithm", "(md5|sha1)");
        String protocol = entryParam.getValue("protocol", "http").trim();
        protocol = ParamsUtils.checked(protocol, "protocol", "https?");
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        return single ? new QueryHash(getQiniuConfig(), algorithm, protocol, domain, urlIndex)
                : new QueryHash(getQiniuConfig(), algorithm, protocol, domain, urlIndex, savePath);
    }

    private ILineProcess<Map<String, String>> getStatFile(boolean single) throws IOException {
        return single ? new StatFile(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, rmFields, saveFormat, saveSeparator)
                : new StatFile(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, rmFields, savePath, saveFormat, saveSeparator);
    }

    private ILineProcess<Map<String, String>> getPrivateUrl(Map<String, String> indexMap, boolean single) throws IOException {
        String domain = entryParam.getValue("domain", "").trim();
        String protocol = entryParam.getValue("protocol", "http").trim();
        protocol = ParamsUtils.checked(protocol, "protocol", "https?");
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        String expires = entryParam.getValue("expires", "3600").trim();
        expires = ParamsUtils.checked(expires, "expires", "[1-9]\\d*");
        return single ? new PrivateUrl(qiniuAccessKey, qiniuSecretKey, domain, protocol, urlIndex, Long.valueOf(expires))
                : new PrivateUrl(qiniuAccessKey, qiniuSecretKey, domain, protocol, urlIndex, Long.valueOf(expires), savePath);
    }

    private ILineProcess<Map<String, String>> getMirrorFile(boolean single) throws IOException {
        return single ? new MirrorFile(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket)
                : new MirrorFile(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, savePath);
    }

    private ILineProcess<Map<String, String>> getExportTs(Map<String, String> indexMap, boolean single) throws IOException {
        String domain = entryParam.getValue("domain", "").trim();
        String protocol = entryParam.getValue("protocol", "http").trim();
        protocol = ParamsUtils.checked(protocol, "protocol", "https?");
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        return single ? new ExportTS(getQiniuConfig(), domain, protocol, urlIndex)
                : new ExportTS(getQiniuConfig(), domain, protocol, urlIndex, savePath);
    }

    public com.qiniu.process.tencent.PrivateUrl getTencentPrivateUrl(boolean single) throws IOException {
        String secretId = entryParam.getValue("ten-id", commonParams.getTencentSecretId());
        String secretKey = entryParam.getValue("ten-secret", commonParams.getTencentSecretKey());
        String tenBucket = entryParam.getValue("ten-bucket", bucket);
        String region = entryParam.getValue("ten-region", regionName);
        if (region == null || "".equals(region)) region = CloudAPIUtils.getTenCosRegion(secretId, secretKey, tenBucket);
        String expires = entryParam.getValue("expires", "3600").trim();
        expires = ParamsUtils.checked(expires, "expires", "[1-9]\\d*");
        return single ? new com.qiniu.process.tencent.PrivateUrl(secretId, secretKey, bucket, region, Long.valueOf(expires)) :
                new com.qiniu.process.tencent.PrivateUrl(secretId, secretKey, bucket, regionName, Long.valueOf(expires), savePath);
    }

    public com.qiniu.process.aliyun.PrivateUrl getAliyunPrivateUrl(boolean single) throws IOException {
        String accessId = entryParam.getValue("ali-id", commonParams.getAliyunAccessId());
        String accessSecret = entryParam.getValue("ali-secret", commonParams.getAliyunAccessSecret());
        String aliBucket = entryParam.getValue("ali-bucket", bucket);
        String region = entryParam.getValue("ali-region", regionName);
        if (region == null || "".equals(region)) region = CloudAPIUtils.getAliOssRegion(accessId, accessSecret, aliBucket);
        String endPoint;
        if (region.matches("https?://.+")) {
            endPoint = region;
        } else {
            if (!region.startsWith("oss-")) region = "oss-" + region;
            endPoint = "http://" + region + ".aliyuncs.com";
        }
        String expires = entryParam.getValue("expires", "3600").trim();
        expires = ParamsUtils.checked(expires, "expires", "[1-9]\\d*");
        return single ? new com.qiniu.process.aliyun.PrivateUrl(accessId, accessSecret, bucket, endPoint, Long.valueOf(expires)) :
                new com.qiniu.process.aliyun.PrivateUrl(accessId, accessSecret, bucket, endPoint, Long.valueOf(expires), savePath);
    }

    public com.qiniu.process.aws.PrivateUrl getAwsS3PrivateUrl(boolean single) throws IOException {
        String accessId = entryParam.getValue("s3-id", commonParams.getS3AccessId());
        String secretKey = entryParam.getValue("s3-secret", commonParams.getS3SecretKey());
        String s3Bucket = entryParam.getValue("s3-bucket", bucket);
        String region = entryParam.getValue("s3-region", regionName);
        if (region == null || "".equals(region)) region = CloudAPIUtils.getS3Region(accessId, secretKey, s3Bucket);
        String expires = entryParam.getValue("expires", "3600").trim();
        expires = ParamsUtils.checked(expires, "expires", "[1-9]\\d*");
        return single ? new com.qiniu.process.aws.PrivateUrl(accessId, secretKey, bucket, region, Long.valueOf(expires)) :
                new com.qiniu.process.aws.PrivateUrl(accessId, secretKey, bucket, regionName, Long.valueOf(expires), savePath);
    }

    public ILineProcess<Map<String, String>> getDownloadFile(Map<String, String> indexMap, boolean single) throws IOException {
        String domain = entryParam.getValue("domain", "").trim();
        String protocol = entryParam.getValue("protocol", "http").trim();
        protocol = ParamsUtils.checked(protocol, "protocol", "https?");
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        String host = entryParam.getValue("host", "").trim();
        String preDown = entryParam.getValue("pre-down", "false").trim();
        preDown = ParamsUtils.checked(preDown, "pre-down", "(true|false)");
        String addPrefix = entryParam.getValue("add-prefix", null);
        String rmPrefix = entryParam.getValue("rm-prefix", null);
        String timeOut = entryParam.getValue("download-timeout", null);
        Configuration configuration = null;
        if (timeOut != null) {
            configuration = new Configuration();
            configuration.connectTimeout = getQiniuConfig().connectTimeout;
            configuration.readTimeout = Integer.valueOf(timeOut);
        }
        return single ? new DownloadFile(configuration, domain, protocol, urlIndex, host, "true".equals(preDown) ? null : savePath,
                addPrefix, rmPrefix) : new DownloadFile(configuration, domain, protocol, urlIndex, host, Boolean.valueOf(preDown),
                addPrefix, rmPrefix, savePath);
    }

    private ILineProcess<Map<String, String>> getImageCensor(Map<String, String> indexMap, boolean single) throws IOException {
        String domain = entryParam.getValue("domain", "").trim();
        String protocol = entryParam.getValue("protocol", "http").trim();
        protocol = ParamsUtils.checked(protocol, "protocol", "https?");
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        Scenes scenes = Scenes.valueOf(entryParam.getValue("scenes").trim());
        return single ? new ImageCensor(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), domain, protocol, urlIndex, scenes) :
                new ImageCensor(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), domain, protocol, urlIndex, scenes, savePath);
    }

    private ILineProcess<Map<String, String>> getVideoCensor(Map<String, String> indexMap, boolean single) throws IOException {
        String domain = entryParam.getValue("domain", "").trim();
        String protocol = entryParam.getValue("protocol", "http").trim();
        protocol = ParamsUtils.checked(protocol, "protocol", "https?");
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        Scenes scenes = Scenes.valueOf(entryParam.getValue("scenes").trim());
        String interval = entryParam.getValue("interval", "0").trim();
        String saverBucket = entryParam.getValue("save-bucket", "").trim();
        String saverPrefix = entryParam.getValue("saver-prefix", "").trim();
        String hookUrl = entryParam.getValue("callback-url", "0").trim();
        return single ? new VideoCensor(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), domain, protocol, urlIndex, scenes,
                Integer.valueOf(interval), saverBucket, saverPrefix, hookUrl) : new VideoCensor(qiniuAccessKey, qiniuSecretKey,
                getQiniuConfig(), domain, protocol, urlIndex, scenes, Integer.valueOf(interval), saverBucket, saverPrefix, hookUrl,
                savePath);
    }

    private ILineProcess<Map<String, String>> getCensorResult(Map<String, String> indexMap, boolean single) throws IOException {
        String jobIdIndex = indexMap.containsValue("id") ? "id" : null;
        return single ? new QueryCensorResult(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), jobIdIndex)
                : new QueryCensorResult(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), jobIdIndex, savePath);
    }
}
