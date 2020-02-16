package com.qiniu.entry;

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.common.comm.Protocol;
import com.baidubce.services.bos.BosClientConfiguration;
import com.google.gson.JsonObject;
import com.obs.services.ObsConfiguration;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.http.HttpProtocol;
import com.qcloud.cos.region.Region;
import com.qiniu.common.Constants;
import com.qiniu.common.SuitsException;
import com.qiniu.convert.MapToString;
import com.qiniu.datasource.*;
import com.qiniu.interfaces.*;
import com.qiniu.process.filtration.*;
import com.qiniu.process.other.*;
import com.qiniu.process.qiniu.*;
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
    private ObsConfiguration obsConfiguration;
    private BosClientConfiguration bosClientConfiguration;
    private boolean httpsConfigEnabled;
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
        this.httpsConfigEnabled = commonParams.isHttpsForConfigEnabled();
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

    public Configuration getQiniuConfig() throws IOException {
        return qiniuConfig == null ? getDefaultQiniuConfig() : qiniuConfig;
    }

    private Configuration getDefaultQiniuConfig() throws IOException {
        if (qiniuAccessKey == null || "".equals(qiniuAccessKey)) {
            qiniuAccessKey = entryParam.getValue("ak").trim();
            qiniuSecretKey = entryParam.getValue("sk").trim();
        }
        return getDefaultQiniuConfig(qiniuAccessKey, qiniuSecretKey, regionName, bucket);
    }

    private Configuration getDefaultQiniuConfig(String ak, String sk, String regionName, String bucket) throws IOException {
        com.qiniu.storage.Region region = CloudApiUtils.getQiniuRegion(regionName);
        String rsfDomain = entryParam.getValue("rsf-domain", null);
        String rsDomain = entryParam.getValue("rs-domain", null);
        String apiDomain = entryParam.getValue("api-domain", null);
        if (rsfDomain != null || rsDomain != null || apiDomain != null) {
            com.qiniu.storage.Region.Builder builder = new com.qiniu.storage.Region.Builder();
            if (rsfDomain != null) region = builder.rsfHost(rsfDomain).build();
            if (rsDomain != null) region = builder.rsHost(rsDomain).build();
            if (apiDomain != null) region = builder.apiHost(apiDomain).build();
        } else {
            region = (regionName == null || "".equals(regionName)) ?
                    CloudApiUtils.getQiniuRegion(CloudApiUtils.getQiniuRegion(ak, sk, bucket))
                    : CloudApiUtils.getQiniuRegion(regionName);
        }
        Configuration configuration = new Configuration(region);
        if (connectTimeout > Constants.CONNECT_TIMEOUT) configuration.connectTimeout = connectTimeout;
        if (readTimeout> Constants.READ_TIMEOUT) configuration.readTimeout = readTimeout;
        if (requestTimeout > Constants.WRITE_TIMEOUT) configuration.writeTimeout = requestTimeout;
        configuration.useHttpsDomains = httpsConfigEnabled;
        return configuration;
    }

    private Configuration getNewQiniuConfig() throws IOException {
        com.qiniu.storage.Region region = CloudApiUtils.getQiniuRegion(regionName);
        String rsfDomain = entryParam.getValue("rsf-domain", null);
        String rsDomain = entryParam.getValue("rs-domain", null);
        String apiDomain = entryParam.getValue("api-domain", null);
        if (rsfDomain != null || rsDomain != null || apiDomain != null) {
            com.qiniu.storage.Region.Builder builder = new com.qiniu.storage.Region.Builder();
            if (rsfDomain != null) region = builder.rsfHost(rsfDomain).build();
            if (rsDomain != null) region = builder.rsHost(rsDomain).build();
            if (apiDomain != null) region = builder.apiHost(apiDomain).build();
        }
        Configuration configuration = new Configuration(region);
        if (connectTimeout > Constants.CONNECT_TIMEOUT) configuration.connectTimeout = connectTimeout;
        if (readTimeout> Constants.READ_TIMEOUT) configuration.readTimeout = readTimeout;
        if (requestTimeout > Constants.WRITE_TIMEOUT) configuration.writeTimeout = requestTimeout;
        configuration.useHttpsDomains = httpsConfigEnabled;
        return configuration;
    }

    public ClientConfig getTenClientConfig() throws IOException {
        return tenClientConfig == null ? getDefaultTenClientConfig() : tenClientConfig;
    }

    private ClientConfig getDefaultTenClientConfig() throws SuitsException {
        if (regionName == null || "".equals(regionName)) regionName = CloudApiUtils.getTenCosRegion(
                commonParams.getTencentSecretId(), commonParams.getTencentSecretKey(), bucket);
        ClientConfig clientConfig = new ClientConfig(new Region(regionName));
        if (1000 * connectTimeout > clientConfig.getConnectionTimeout())
            clientConfig.setConnectionTimeout(1000 * connectTimeout);
        if (1000 * readTimeout > clientConfig.getSocketTimeout())
            clientConfig.setSocketTimeout(1000 * readTimeout);
        if (1000 * requestTimeout > clientConfig.getConnectionRequestTimeout())
            clientConfig.setConnectionRequestTimeout(1000 * requestTimeout);
        if (httpsConfigEnabled) clientConfig.setHttpProtocol(HttpProtocol.https);
        else clientConfig.setHttpProtocol(HttpProtocol.http);
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
        if (httpsConfigEnabled) clientConfig.setProtocol(Protocol.HTTPS);
        else clientConfig.setProtocol(Protocol.HTTP);
        return clientConfig;
    }

    public UpYunConfig getUpYunConfig() {
        return upYunConfig == null ? getDefaultUpYunConfig() : upYunConfig;
    }

    private UpYunConfig getDefaultUpYunConfig() {
        UpYunConfig upYunConfig = new UpYunConfig(httpsConfigEnabled);
        if (1000 * connectTimeout > upYunConfig.getConnectTimeout())
            upYunConfig.setConnectTimeout(1000 * connectTimeout);
        if (1000 * readTimeout > upYunConfig.getReadTimeout())
            upYunConfig.setReadTimeout(1000 * readTimeout);
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
        if (httpsConfigEnabled) clientConfig.setProtocol(com.amazonaws.Protocol.HTTPS);
        else clientConfig.setProtocol(com.amazonaws.Protocol.HTTP);
        return clientConfig;
    }

    public ObsConfiguration getObsConfiguration() {
        return obsConfiguration == null ? getDefaultObsConfiguration() : obsConfiguration;
    }

    private ObsConfiguration getDefaultObsConfiguration() {
        ObsConfiguration obsConfiguration = new ObsConfiguration();
        if (1000 * connectTimeout > obsConfiguration.getConnectionTimeout())
            obsConfiguration.setConnectionTimeout(1000 * connectTimeout);
        if (1000 * readTimeout > obsConfiguration.getSocketTimeout())
            obsConfiguration.setSocketTimeout(1000 * readTimeout);
//        if (1000 * requestTimeout > obsConfiguration.getConnectionRequestTimeout())
//            obsConfiguration.setConnectionRequestTimeout(1000 * requestTimeout);
        obsConfiguration.setHttpsOnly(httpsConfigEnabled);
        return obsConfiguration;
    }

    private BosClientConfiguration getBosClientConfiguration() {
        return bosClientConfiguration == null ? getDefaultBosClientConfiguration() : bosClientConfiguration;
    }

    private BosClientConfiguration getDefaultBosClientConfiguration() {
        BosClientConfiguration bosClientConfiguration = new BosClientConfiguration();
        if (1000 * connectTimeout > bosClientConfiguration.getConnectionTimeoutInMillis())
            bosClientConfiguration.setConnectionTimeoutInMillis(1000 * connectTimeout);
        if (1000 * readTimeout > bosClientConfiguration.getSocketTimeoutInMillis())
            bosClientConfiguration.setSocketTimeoutInMillis(1000 * readTimeout);
        if (httpsConfigEnabled) bosClientConfiguration.setProtocol(com.baidubce.Protocol.HTTPS);
        else bosClientConfiguration.setProtocol(com.baidubce.Protocol.HTTP);
        return bosClientConfiguration;
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
        } else if ("huawei".equals(source)) {
            return getHuaweiObsContainer();
        } else if ("baidu".equals(source)) {
            return getBaiduBosContainer();
        } else if ("local".equals(source)) {
            if (commonParams.isSelfUpload() || "file".equals(commonParams.getParse())) return getDefaultFileContainer();
            else return getTextFileContainer();
        } else {
            throw new IOException("no such datasource: " + source);
        }
    }

    public InputSource getInputSource() {
        String parse = commonParams.getParse();
        String separator = commonParams.getSeparator();
        String addKeyPrefix = commonParams.getAddKeyPrefix();
        String rmKeyPrefix = commonParams.getRmKeyPrefix();
        return new InputSource(parse, separator, addKeyPrefix, rmKeyPrefix, indexMap);
    }

    public TextFileContainer getTextFileContainer() throws IOException {
        String filePath = commonParams.getPath();
        String parse = commonParams.getParse();
        String separator = commonParams.getSeparator();
        String split = entryParam.getValue("auto-split", "false");
        ParamsUtils.checked(split, "auto-split", "(true|false)");
        boolean autoSlit = Boolean.parseBoolean(split);
        String addKeyPrefix = commonParams.getAddKeyPrefix();
        String rmKeyPrefix = commonParams.getRmKeyPrefix();
        Map<String, Map<String, String>> urisMap = commonParams.getPathConfigMap();
        TextFileContainer textFileContainer = new TextFileContainer(filePath, parse, separator, urisMap,
                commonParams.getAntiPrefixes(), autoSlit, addKeyPrefix, rmKeyPrefix, indexMap, null, unitLen, threads);
        textFileContainer.setSaveOptions(saveTotal, savePath, saveFormat, saveSeparator, rmFields);
        textFileContainer.setRetryTimes(retryTimes);
        return textFileContainer;
    }

    public DefaultFileContainer getDefaultFileContainer() throws IOException {
        String path = commonParams.getPath();
        Map<String, Map<String, String>> directoriesMap = commonParams.getPathConfigMap();
        List<String> antiDirectories = commonParams.getAntiPrefixes();
        boolean keepDir = commonParams.getKeepDir();
        DefaultFileContainer defaultFileContainer = new DefaultFileContainer(path, directoriesMap, antiDirectories,
                keepDir, indexMap, null, unitLen, threads);
        defaultFileContainer.setSaveOptions(saveTotal, savePath, saveFormat, saveSeparator, rmFields);
        defaultFileContainer.setRetryTimes(retryTimes);
        return defaultFileContainer;
    }

    public QiniuQosContainer getQiniuQosContainer() throws IOException {
        if (qiniuConfig == null) qiniuConfig = getDefaultQiniuConfig();
        Map<String, Map<String, String>> prefixesMap = commonParams.getPathConfigMap();
        List<String> antiPrefixes = commonParams.getAntiPrefixes();
        boolean prefixLeft = commonParams.getPrefixLeft();
        boolean prefixRight = commonParams.getPrefixRight();
        QiniuQosContainer qiniuQosContainer = new QiniuQosContainer(qiniuAccessKey, qiniuSecretKey, qiniuConfig, bucket,
                prefixesMap, antiPrefixes, prefixLeft, prefixRight, indexMap, null, unitLen, threads);
        qiniuQosContainer.setSaveOptions(saveTotal, savePath, saveFormat, saveSeparator, rmFields);
        qiniuQosContainer.setRetryTimes(retryTimes);
        return qiniuQosContainer;
    }

    public TenCosContainer getTenCosContainer() throws IOException {
        String secretId = commonParams.getTencentSecretId();
        String secretKey = commonParams.getTencentSecretKey();
        if (tenClientConfig == null) tenClientConfig = getDefaultTenClientConfig();
        Map<String, Map<String, String>> prefixesMap = commonParams.getPathConfigMap();
        List<String> antiPrefixes = commonParams.getAntiPrefixes();
        boolean prefixLeft = commonParams.getPrefixLeft();
        boolean prefixRight = commonParams.getPrefixRight();
        TenCosContainer tenCosContainer = new TenCosContainer(secretId, secretKey, tenClientConfig, bucket,
                prefixesMap, antiPrefixes, prefixLeft, prefixRight, indexMap, null, unitLen, threads);
        tenCosContainer.setSaveOptions(saveTotal, savePath, saveFormat, saveSeparator, rmFields);
        tenCosContainer.setRetryTimes(retryTimes);
        return tenCosContainer;
    }

    public AliOssContainer getAliOssContainer() throws IOException {
        String accessId = commonParams.getAliyunAccessId();
        String accessSecret = commonParams.getAliyunAccessSecret();
        String endPoint;
        if (regionName == null || "".equals(regionName)) regionName = CloudApiUtils.getAliOssRegion(accessId, accessSecret, bucket);
        if (regionName.matches("https?://.+")) {
            endPoint = regionName;
        } else {
            if (!regionName.startsWith("oss-")) regionName = "oss-" + regionName;
            endPoint = "http://" + regionName + ".aliyuncs.com";
        }
        if (aliClientConfig == null) aliClientConfig = getDefaultAliClientConfig();
        Map<String, Map<String, String>> prefixesMap = commonParams.getPathConfigMap();
        List<String> antiPrefixes = commonParams.getAntiPrefixes();
        boolean prefixLeft = commonParams.getPrefixLeft();
        boolean prefixRight = commonParams.getPrefixRight();
        AliOssContainer aliOssContainer = new AliOssContainer(accessId, accessSecret, aliClientConfig, endPoint, bucket,
                prefixesMap, antiPrefixes, prefixLeft, prefixRight, indexMap, null, unitLen, threads);
        aliOssContainer.setSaveOptions(saveTotal, savePath, saveFormat, saveSeparator, rmFields);
        aliOssContainer.setRetryTimes(retryTimes);
        return aliOssContainer;
    }

    public UpYosContainer getUpYosContainer() throws IOException {
        String username = commonParams.getUpyunUsername();
        String password = commonParams.getUpyunPassword();
        if (upYunConfig == null) upYunConfig = getDefaultUpYunConfig();
        Map<String, Map<String, String>> prefixesMap = commonParams.getPathConfigMap();
        List<String> antiPrefixes = commonParams.getAntiPrefixes();
//        boolean prefixLeft = commonParams.getPrefixLeft();
//        boolean prefixRight = commonParams.getPrefixRight();
        UpYosContainer upYosContainer = new UpYosContainer(username, password, upYunConfig, bucket,  prefixesMap, antiPrefixes,
//                prefixLeft, prefixRight,
                indexMap, null, unitLen, threads);
        upYosContainer.setSaveOptions(saveTotal, savePath, saveFormat, saveSeparator, rmFields);
        upYosContainer.setRetryTimes(retryTimes);
        return upYosContainer;
    }

    public AwsS3Container getAwsS3Container() throws IOException {
        String s3AccessId = commonParams.getS3AccessId();
        String s3SecretKey = commonParams.getS3SecretKey();
        if (s3ClientConfig == null) s3ClientConfig = getDefaultS3ClientConfig();
        Map<String, Map<String, String>> prefixesMap = commonParams.getPathConfigMap();
        List<String> antiPrefixes = commonParams.getAntiPrefixes();
        boolean prefixLeft = commonParams.getPrefixLeft();
        boolean prefixRight = commonParams.getPrefixRight();
        String endpoint = entryParam.getValue("endpoint", "").trim();
        if (endpoint.isEmpty() && (regionName == null || "".equals(regionName)))
            regionName = CloudApiUtils.getS3Region(s3AccessId, s3SecretKey, bucket);
        AwsS3Container awsS3Container = new AwsS3Container(s3AccessId, s3SecretKey, s3ClientConfig, endpoint, regionName, bucket,
                prefixesMap, antiPrefixes, prefixLeft, prefixRight, indexMap, null, unitLen, threads);
        awsS3Container.setSaveOptions(saveTotal, savePath,  saveFormat, saveSeparator, rmFields);
        awsS3Container.setRetryTimes(retryTimes);
        return awsS3Container;
    }

    public HuaweiObsContainer getHuaweiObsContainer() throws IOException {
        String accessId = commonParams.getHuaweiAccessId();
        String secretKey = commonParams.getHuaweiSecretKey();
        if (obsConfiguration == null) obsConfiguration = getDefaultObsConfiguration();
        Map<String, Map<String, String>> prefixesMap = commonParams.getPathConfigMap();
        List<String> antiPrefixes = commonParams.getAntiPrefixes();
        boolean prefixLeft = commonParams.getPrefixLeft();
        boolean prefixRight = commonParams.getPrefixRight();
        String endPoint;
        if (regionName == null || "".equals(regionName)) regionName = CloudApiUtils.getHuaweiObsRegion(accessId, secretKey, bucket);
        if (regionName.matches("https?://.+")) {
            endPoint = regionName;
        } else {
            if (!regionName.startsWith("obs.")) regionName = "obs." + regionName;
            endPoint = "http://" + regionName + ".myhuaweicloud.com";
        }
        HuaweiObsContainer huaweiObsContainer = new HuaweiObsContainer(accessId, secretKey, obsConfiguration, endPoint,
                bucket, prefixesMap, antiPrefixes, prefixLeft, prefixRight, indexMap, null, unitLen,
                threads);
        huaweiObsContainer.setSaveOptions(saveTotal, savePath,  saveFormat, saveSeparator, rmFields);
        huaweiObsContainer.setRetryTimes(retryTimes);
        return huaweiObsContainer;
    }

    public BaiduBosContainer getBaiduBosContainer() throws IOException {
        String accessId = commonParams.getBaiduAccessId();
        String secretKey = commonParams.getBaiduSecretKey();
        if (bosClientConfiguration == null) bosClientConfiguration = getDefaultBosClientConfiguration();
        Map<String, Map<String, String>> prefixesMap = commonParams.getPathConfigMap();
        List<String> antiPrefixes = commonParams.getAntiPrefixes();
        boolean prefixLeft = commonParams.getPrefixLeft();
        boolean prefixRight = commonParams.getPrefixRight();
        String endPoint;
        if (regionName == null || "".equals(regionName)) regionName = CloudApiUtils.getHuaweiObsRegion(accessId, secretKey, bucket);
        if (regionName.matches("https?://.+")) {
            endPoint = regionName;
        } else {
            endPoint = "http://" + regionName + ".bcebos.com";
        }
        BaiduBosContainer baiduBosContainer = new BaiduBosContainer(accessId, secretKey, bosClientConfiguration, endPoint,
                bucket, prefixesMap, antiPrefixes, prefixLeft, prefixRight, indexMap, null, unitLen, threads);
        baiduBosContainer.setSaveOptions(saveTotal, savePath,  saveFormat, saveSeparator, rmFields);
        baiduBosContainer.setRetryTimes(retryTimes);
        return baiduBosContainer;
    }

    public ILineProcess<Map<String, String>> getProcessor() throws Exception {
        ILineProcess<Map<String, String>> processor = process == null ? null : whichNextProcessor(false);
        BaseFilter<Map<String, String>> baseFilter = commonParams.getBaseFilter();
        SeniorFilter<Map<String, String>> seniorFilter = commonParams.getSeniorFilter();
        if (baseFilter != null || seniorFilter != null) {
            String strictError = entryParam.getValue("f-strict-error", "false").trim();
            ParamsUtils.checked(strictError, "f-strict-error", "(true|false)");
            List<String> fields = ConvertingUtils.getOrderedFields(indexMap, rmFields);
            FilterProcess<Map<String, String>> filterProcessor;
            if ("true".equals(strictError) || processor == null) {
                filterProcessor = new FilterProcess<Map<String, String>>(baseFilter, seniorFilter, savePath) {
                    @Override
                    protected ITypeConvert<Map<String, String>, String> newPersistConverter() throws IOException {
                        return new MapToString(saveFormat, saveSeparator, fields);
                    }
                };
                filterProcessor.setStrictError(Boolean.parseBoolean(strictError));
            } else {
                filterProcessor = new FilterProcess<Map<String, String>>(baseFilter, seniorFilter){};
            }
            if (processor != null) filterProcessor.setNextProcessor(processor);
            return filterProcessor;
        } else {
            if ("filter".equals(process)) {
                throw new Exception("please set the correct filter conditions.");
            } else {
                return processor;
            }
        }
    }

    public ILineProcess<Map<String, String>> whichNextProcessor(boolean single) throws Exception {
        ILineProcess<Map<String, String>> processor = null;
        ILineProcess<Map<String, String>> privateProcessor = null;
        boolean useQuery = true;
        Map<String, String> indexes = new HashMap<>(indexMap);
        if (ProcessUtils.canPrivateToNext(process)) {
            privateProcessor = getPrivateTypeProcessor(single);
            if (privateProcessor != null) {
                indexes.put("url", "url");
                single = true;
                useQuery = false; // 签名之后的 url 不能在使用 query 参数
            }
        }
        switch (process) {
            case "status": processor = getChangeStatus(single); break;
            case "type": processor = getChangeType(single); break;
            case "lifecycle": processor = getChangeLifecycle(single); break;
            case "copy": processor = getCopyFile(indexes, single); break;
            case "move":
            case "rename": processor = getMoveFile(indexes, single); break;
            case "delete": processor = getDeleteFile(single); break;
            case "asyncfetch": processor = getAsyncFetch(indexes, single); break;
            case "avinfo": processor = getQueryAvinfo(indexes, single); break;
            case "pfop": processor = getPfop(indexes, single); break;
            case "pfopcmd": processor = getPfopCommand(indexes, single); break;
            case "pfopresult": processor = getPfopResult(indexes, single); break;
            case "qhash": processor = getQueryHash(indexes, single); break;
            case "stat": processor = getStatFile(single); break;
            case "privateurl": processor = getPrivateUrl(indexes, single); break;
            case "publicurl": processor = getPublicUrl(single); break;
            case "mirror": processor = getMirrorFile(single); break;
            case "exportts": processor = getExportTs(indexes, single); break;
            case "tenprivate": processor = getTencentPrivateUrl(single); break;
            case "s3private": case "awsprivate": processor = getAwsS3PrivateUrl(single); break;
            case "aliprivate": processor = getAliyunPrivateUrl(single); break;
            case "huaweiprivate": processor = getHuaweiPrivateUrl(single); break;
            case "baiduprivate": processor = getBaiduPrivateUrl(single); break;
            case "download": processor = getDownloadFile(indexes, single, useQuery); break;
            case "imagecensor": processor = getImageCensor(indexes, single, useQuery); break;
            case "videocensor": processor = getVideoCensor(indexes, single); break;
            case "censorresult": processor = getCensorResult(indexes, single); break;
            case "qupload": processor = getQiniuUploadFile(indexes, single); break;
            case "mime": processor = getChangeMime(indexes, single); break;
            case "metadata": processor = getChangeMetadata(single); break;
            case "cdnrefresh": processor = getCdnRefresh(indexes, single); break;
            case "cdnprefetch": processor = getCdnPrefetch(indexes, single); break;
            case "refreshquery": processor = getRefreshQuery(indexes, single); break;
            case "prefetchquery": processor = getPrefetchQuery(indexes, single); break;
            case "fetch": processor = getFetch(indexes, single); break;
            case "syncupload": processor = getSyncUpload(indexes, single); break;
            case "filter": case "": break;
            case "domainsofbucket": processor = getDomainsOfBucket(single); break;
            default: throw new IOException("unsupported process: " + process);
        }
        if (processor != null) {
            if (ProcessUtils.canBatch(processor.getProcessName())) processor.setBatchSize(commonParams.getBatchSize());
            // 为了保证程序出现因网络等原因产生的非预期异常时正常运行需要设置重试次数
            processor.setRetryTimes(retryTimes);
            processor.setCheckType(entryParam.getValue("check", "").trim());
            if (privateProcessor != null) {
                privateProcessor.setNextProcessor(processor);
                return privateProcessor;
            }
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
        } else if ("huawei".equals(privateType)) {
            processor = getHuaweiPrivateUrl(single);
        } else if ("baidu".equals(privateType)) {
            processor = getBaiduPrivateUrl(single);
        } else if (privateType != null && !"".equals(privateType)) {
            throw new IOException("unsupported private process: " + privateType + " for asyncfetch's url.");
        }
        return processor;
    }

    private ILineProcess<Map<String, String>> getChangeStatus(boolean single) throws IOException {
        String status = entryParam.getValue("status").trim();
        ParamsUtils.checked(status, "status", "[01]");
        return single ? new ChangeStatus(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, Integer.parseInt(status))
                : new ChangeStatus(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, Integer.parseInt(status), savePath);
    }

    private ILineProcess<Map<String, String>> getChangeType(boolean single) throws IOException {
        String type = entryParam.getValue("type").trim();
        ParamsUtils.checked(type, "type", "[01]");
        return single ? new ChangeType(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, Integer.parseInt(type))
                : new ChangeType(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, Integer.parseInt(type), savePath);
    }

    private ILineProcess<Map<String, String>> getChangeLifecycle(boolean single) throws IOException {
        String days = entryParam.getValue("days").trim();
        ParamsUtils.checked(days, "days", "\\d+");
        return single ? new ChangeLifecycle(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, Integer.parseInt(days))
                : new ChangeLifecycle(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, Integer.parseInt(days), savePath);
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
        String toBucket = "move".equals(process) ?
                entryParam.getValue("to-bucket").trim() : entryParam.getValue("to-bucket", "").trim();
        String toKeyIndex = indexMap.containsValue("toKey") ? "toKey" : null;
        String addPrefix = entryParam.getValue("add-prefix", null);
        String rmPrefix = entryParam.getValue("rm-prefix", null);
        String force = entryParam.getValue("prefix-force", "false").trim();
        ParamsUtils.checked(force, "prefix-force", "(true|false)");
        return single ? new MoveFile(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, toBucket, toKeyIndex, addPrefix,
                rmPrefix, Boolean.parseBoolean(force))
                : new MoveFile(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, toBucket, toKeyIndex, addPrefix,
                rmPrefix, Boolean.parseBoolean(force), savePath);
    }

    private ILineProcess<Map<String, String>> getDeleteFile(boolean single) throws IOException {
        return single ? new DeleteFile(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket)
                : new DeleteFile(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, savePath);
    }

    private ILineProcess<Map<String, String>> getAsyncFetch(Map<String, String> indexMap, boolean single) throws IOException {
        String ak = qiniuAccessKey == null || qiniuAccessKey.isEmpty() ?
                entryParam.getValue("qiniu-ak").trim() : qiniuAccessKey;
        String sk = qiniuSecretKey == null || qiniuSecretKey.isEmpty() ?
                entryParam.getValue("qiniu-sk").trim() : qiniuSecretKey;
        String toBucket = entryParam.getValue("to-bucket").trim();
        if (toBucket.equals(bucket) && "qiniu".equals(source))
            throw new IOException("the to-bucket can not be same as bucket if source is qiniu.");
        String protocol = entryParam.getValue("protocol", "http").trim();
        ParamsUtils.checked(protocol, "protocol", "https?");
        String domain = entryParam.getValue("domain", "").trim();
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        String addPrefix = entryParam.getValue("add-prefix", null);
        String rmPrefix = entryParam.getValue("rm-prefix", null);
        String host = entryParam.getValue("host", "").trim();
        String md5Index = entryParam.getValue("md5-index", "").trim();
        String callbackUrl = entryParam.getValue("callback-url", "").trim();
        String checkUrl = entryParam.getValue("check-url", "true").trim();
        if ("true".equals(checkUrl) && !"".equals(callbackUrl)) RequestUtils.checkCallbackUrl(callbackUrl);
        String callbackBody = entryParam.getValue("callback-body", "").trim();
        String callbackBodyType = entryParam.getValue("callback-body-type", "").trim();
        String callbackHost = entryParam.getValue("callback-host", "").trim();
        String type = entryParam.getValue("file-type", "0").trim();
        String ignore = entryParam.getValue("ignore-same-key", "false").trim();
        ParamsUtils.checked(ignore, "ignore-same-key", "(true|false)");
        String regionStr = entryParam.getValue("qiniu-region", regionName).trim();
        Configuration configuration = getDefaultQiniuConfig(ak, sk, regionStr, toBucket);
        AsyncFetch processor = single ? new AsyncFetch(ak, sk, configuration, toBucket, protocol, domain, urlIndex,
                addPrefix, rmPrefix) : new AsyncFetch(ak, sk, configuration, toBucket, protocol, domain, urlIndex,
                addPrefix, rmPrefix, savePath);
        if (!host.isEmpty() || !md5Index.isEmpty() || !callbackUrl.isEmpty() || !callbackBody.isEmpty() ||
                !callbackBodyType.isEmpty() || !callbackHost.isEmpty() || "1".equals(type) || "true".equals(ignore)) {
            processor.setFetchArgs(host, md5Index, callbackUrl, callbackBody,
                    callbackBodyType, callbackHost, Integer.parseInt(type), Boolean.parseBoolean(ignore));
        }
        return processor;
    }

    private ILineProcess<Map<String, String>> getQueryAvinfo(Map<String, String> indexMap, boolean single) throws IOException {
        String protocol = entryParam.getValue("protocol", "http").trim();
        ParamsUtils.checked(protocol, "protocol", "https?");
        String domain = entryParam.getValue("domain", "").trim();
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        return single ? new QueryAvinfo(getNewQiniuConfig(), protocol, domain, urlIndex)
                : new QueryAvinfo(getNewQiniuConfig(), protocol, domain, urlIndex, savePath);
    }

    private ILineProcess<Map<String, String>> getPfopCommand(Map<String, String> indexMap, boolean single) throws IOException {
        String avinfoIndex = indexMap.containsValue("avinfo") ? "avinfo" : null;
        String duration = entryParam.getValue("duration", "false").trim();
        ParamsUtils.checked(duration, "duration", "(true|false)");
        String size = entryParam.getValue("size", "false").trim();
        ParamsUtils.checked(size, "size", "(true|false)");
        String combine = entryParam.getValue("combine", "true").trim();
        ParamsUtils.checked(combine, "combine", "(true|false)");
        String configJson = entryParam.getValue("pfop-config", "").trim();
        List<JsonObject> pfopConfigs = commonParams.getPfopConfigs();
        return single ? new PfopCommand(getNewQiniuConfig(), avinfoIndex, Boolean.parseBoolean(duration), Boolean.parseBoolean(size),
                Boolean.parseBoolean(combine), configJson, pfopConfigs)
                : new PfopCommand(getNewQiniuConfig(), avinfoIndex, Boolean.parseBoolean(duration), Boolean.parseBoolean(size),
                Boolean.parseBoolean(combine), configJson, pfopConfigs, savePath);
    }

    private ILineProcess<Map<String, String>> getPfop(Map<String, String> indexMap, boolean single) throws IOException {
        String pipeline = entryParam.getValue("pipeline", "").trim();
        String notifyURL = entryParam.getValue("notifyURL", "").trim();
        String force = entryParam.getValue("force", "false").trim();
        ParamsUtils.checked(force, "force", "(true|false)");
        String forcePublic = entryParam.getValue("force-public", "false").trim();
        if (pipeline.isEmpty() && !"true".equals(forcePublic)) {
            throw new IOException("please set pipeline, if you don't want to use" +
                    " private pipeline, please set the force-public as true.");
        }
        String configJson = entryParam.getValue("pfop-config", "").trim();
        List<JsonObject> pfopConfigs = commonParams.getPfopConfigs();
        String fopsIndex = indexMap.containsValue("fops") ? "fops" : null;
        return single ? new QiniuPfop(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, pipeline, notifyURL,
                Boolean.parseBoolean(force), configJson, pfopConfigs, fopsIndex)
                : new QiniuPfop(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, pipeline, notifyURL,
                Boolean.parseBoolean(force), configJson, pfopConfigs, fopsIndex, savePath);
    }

    private ILineProcess<Map<String, String>> getPfopResult(Map<String, String> indexMap, boolean single) throws IOException {
        String protocol = entryParam.getValue("protocol", "http").trim();
        ParamsUtils.checked(protocol, "protocol", "https?");
        String pIdIndex = indexMap.containsValue("id") ? "id" : null;
        return single ? new QueryPfopResult(getNewQiniuConfig(), protocol, pIdIndex)
                : new QueryPfopResult(getNewQiniuConfig(), protocol, pIdIndex, savePath);
    }

    private ILineProcess<Map<String, String>> getQueryHash(Map<String, String> indexMap, boolean single) throws IOException {
        String protocol = entryParam.getValue("protocol", "http").trim();
        ParamsUtils.checked(protocol, "protocol", "https?");
        String domain = entryParam.getValue("domain", "").trim();
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        String algorithm = entryParam.getValue("algorithm", "md5").trim();
        ParamsUtils.checked(algorithm, "algorithm", "(md5|sha1)");
        return single ? new QueryHash(getNewQiniuConfig(), protocol, domain, urlIndex, algorithm)
                : new QueryHash(getNewQiniuConfig(), protocol, domain, urlIndex, algorithm, savePath);
    }

    private ILineProcess<Map<String, String>> getStatFile(boolean single) throws IOException {
        return single ? new StatFile(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, saveFormat, saveSeparator, rmFields)
                : new StatFile(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, savePath, saveFormat, saveSeparator, rmFields);
    }

    private ILineProcess<Map<String, String>> getPrivateUrl(Map<String, String> indexMap, boolean single) throws IOException {
        String protocol = entryParam.getValue("protocol", "http").trim();
        ParamsUtils.checked(protocol, "protocol", "https?");
        String domain = entryParam.getValue("domain", "").trim();
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        String queries = entryParam.getValue("queries", "").trim();
        String expires = entryParam.getValue("expires", "3600").trim();
        ParamsUtils.checked(expires, "expires", "[1-9]\\d*");
        return single ? new PrivateUrl(qiniuAccessKey, qiniuSecretKey, protocol, domain, urlIndex, queries, Long.parseLong(expires))
                : new PrivateUrl(qiniuAccessKey, qiniuSecretKey, protocol, domain, urlIndex, queries, Long.parseLong(expires), savePath);
    }

    private ILineProcess<Map<String, String>> getPublicUrl(boolean single) throws IOException {
        String protocol = entryParam.getValue("protocol", "http").trim();
        ParamsUtils.checked(protocol, "protocol", "https?");
        String domain = entryParam.getValue("domain", "").trim();
        String queries = entryParam.getValue("queries", "").trim();
        return single ? new PublicUrl(qiniuAccessKey, qiniuSecretKey, protocol, domain, queries)
                : new PublicUrl(qiniuAccessKey, qiniuSecretKey, protocol, domain, queries, savePath);
    }

    private ILineProcess<Map<String, String>> getMirrorFile(boolean single) throws IOException {
        String ak = qiniuAccessKey == null || qiniuAccessKey.isEmpty() ?
                entryParam.getValue("qiniu-ak").trim() : qiniuAccessKey;
        String sk = qiniuSecretKey == null || qiniuSecretKey.isEmpty() ?
                entryParam.getValue("qiniu-sk").trim() : qiniuSecretKey;
        String toBucket = entryParam.getValue("to-bucket").trim();
        if (toBucket.equals(bucket) && "qiniu".equals(source))
            throw new IOException("the to-bucket can not be same as bucket if source is qiniu.");
        String regionStr = entryParam.getValue("qiniu-region", regionName).trim();
        Configuration configuration = getDefaultQiniuConfig(ak, sk, regionStr, toBucket);
        return single ? new MirrorFile(ak, sk, configuration, toBucket) : new MirrorFile(ak, sk, configuration, toBucket, savePath);
    }

    private ILineProcess<Map<String, String>> getExportTs(Map<String, String> indexMap, boolean single) throws IOException {
        String protocol = entryParam.getValue("protocol", "http").trim();
        ParamsUtils.checked(protocol, "protocol", "https?");
        String domain = entryParam.getValue("domain", "").trim();
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        return single ? new ExportTS(getNewQiniuConfig(), protocol, domain, urlIndex)
                : new ExportTS(getNewQiniuConfig(), protocol, domain, urlIndex, savePath);
    }

    private Map<String, String> getQueriesMap() {
        String queries = entryParam.getValue("queries", "").trim();
        if (queries.startsWith("\\?")) queries = queries.substring(1);
        String[] items = queries.split("&");
        Map<String, String> queriesMap = new HashMap<>();
        int index;
        String key;
        String value;
        for (String item : items) {
            index = item.indexOf("=");
            if (index < 0) {
                key = item;
                value = "";
            } else {
                key = item.substring(0, index);
                value = item.substring(index + 1);
            }
            queriesMap.put(key, value);
        }
        return queriesMap;
    }

    private com.qiniu.process.tencent.PrivateUrl getTencentPrivateUrl(boolean single) throws IOException {
        String secretId = commonParams.getTencentSecretId();
        String secretKey = commonParams.getTencentSecretKey();
        if (secretId == null || secretId.isEmpty()) {
            secretId = entryParam.getValue("ten-id");
            secretKey = entryParam.getValue("ten-secret");
        }
        String tenBucket = bucket == null || bucket.isEmpty() ? entryParam.getValue("bucket") : bucket;
        String region = regionName == null || regionName.isEmpty() ? entryParam.getValue("region", regionName) : regionName;
        if (region == null || "".equals(region)) region = CloudApiUtils.getTenCosRegion(secretId, secretKey, tenBucket);
        String expires = entryParam.getValue("expires", "3600").trim();
        ParamsUtils.checked(expires, "expires", "[1-9]\\d*");
        return single ? new com.qiniu.process.tencent.PrivateUrl(secretId, secretKey, tenBucket, region, httpsConfigEnabled,
                1000 * Long.parseLong(expires), getQueriesMap()) : new com.qiniu.process.tencent.PrivateUrl(secretId,
                secretKey, tenBucket, region, httpsConfigEnabled,1000 * Long.parseLong(expires), getQueriesMap(), savePath);
    }

    private com.qiniu.process.aliyun.PrivateUrl getAliyunPrivateUrl(boolean single) throws IOException {
        String accessId = commonParams.getAliyunAccessId();
        String accessSecret = commonParams.getAliyunAccessSecret();
        if (accessId == null || accessId.isEmpty()) {
            accessId = entryParam.getValue("ali-id");
            accessSecret = entryParam.getValue("ali-secret");
        }
        String aliBucket = bucket == null || bucket.isEmpty() ? entryParam.getValue("bucket") : bucket;
        String endPoint = regionName == null || regionName.isEmpty() ? entryParam.getValue("region", regionName) : regionName;
        if (endPoint == null || "".equals(endPoint)) endPoint = CloudApiUtils.getAliOssRegion(accessId, accessSecret, aliBucket);
        if (!endPoint.matches("https?://.+")) {
            if (endPoint.startsWith("oss-")) {
                endPoint = String.join(endPoint, httpsConfigEnabled ? "https://" : "http://", ".aliyuncs.com");
            } else {
                endPoint = String.join(endPoint, httpsConfigEnabled ? "https://oss-" : "http://oss-", ".aliyuncs.com");
            }
        }
        String expires = entryParam.getValue("expires", "3600").trim();
        ParamsUtils.checked(expires, "expires", "[1-9]\\d*");
        return single ? new com.qiniu.process.aliyun.PrivateUrl(accessId, accessSecret, aliBucket, endPoint,
                1000 * Long.parseLong(expires), getQueriesMap()) : new com.qiniu.process.aliyun.PrivateUrl(accessId,
                accessSecret, aliBucket, endPoint, 1000 * Long.parseLong(expires), getQueriesMap(), savePath);
    }

    private com.qiniu.process.aws.PrivateUrl getAwsS3PrivateUrl(boolean single) throws IOException {
        String accessId = commonParams.getS3AccessId();
        String secretKey = commonParams.getS3SecretKey();
        if (accessId == null || accessId.isEmpty()) {
            accessId = entryParam.getValue("s3-id");
            secretKey = entryParam.getValue("s3-secret");
        }
        String s3Bucket = bucket == null || bucket.isEmpty() ? entryParam.getValue("bucket") : bucket;
        String region = regionName == null || regionName.isEmpty() ? entryParam.getValue("region", regionName) : regionName;
        String endpoint = entryParam.getValue("endpoint", "").trim();
        if (endpoint.isEmpty() && (region == null || "".equals(region)))
            region = CloudApiUtils.getS3Region(accessId, secretKey, s3Bucket);
        String expires = entryParam.getValue("expires", "3600").trim();
        ParamsUtils.checked(expires, "expires", "[1-9]\\d*");
        return single ? new com.qiniu.process.aws.PrivateUrl(accessId, secretKey, s3Bucket, endpoint, region, httpsConfigEnabled,
                1000 * Long.parseLong(expires), getQueriesMap()) : new com.qiniu.process.aws.PrivateUrl(accessId,
                secretKey, s3Bucket, endpoint, region, httpsConfigEnabled, 1000 * Long.parseLong(expires), getQueriesMap(), savePath);
    }

    private com.qiniu.process.huawei.PrivateUrl getHuaweiPrivateUrl(boolean single) throws IOException {
        String accessId = commonParams.getS3AccessId();
        String secretKey = commonParams.getS3SecretKey();
        if (accessId == null || accessId.isEmpty()) {
            accessId = entryParam.getValue("hua-id");
            secretKey = entryParam.getValue("hua-secret");
        }
        String huaweiBucket = bucket == null || bucket.isEmpty() ? entryParam.getValue("bucket") : bucket;
        String endPoint = regionName == null || regionName.isEmpty() ? entryParam.getValue("region", regionName) : regionName;
        if (endPoint == null || "".equals(endPoint)) endPoint = CloudApiUtils.getHuaweiObsRegion(accessId, secretKey, huaweiBucket);
        if (!endPoint.matches("https?://.+")) {
            if (endPoint.startsWith("obs.")) {
                endPoint = String.join(endPoint, httpsConfigEnabled ? "https://" : "http://", ".myhuaweicloud.com");
            } else {
                endPoint = String.join(endPoint, httpsConfigEnabled ? "https://obs." : "http://obs.", ".myhuaweicloud.com");
            }
        }
        String expires = entryParam.getValue("expires", "3600").trim();
        ParamsUtils.checked(expires, "expires", "[1-9]\\d*");
        // 华为 sdk 的过期时间按秒设置
        return single ? new com.qiniu.process.huawei.PrivateUrl(accessId, secretKey, huaweiBucket, endPoint, Long.parseLong(expires),
                getQueriesMap()) : new com.qiniu.process.huawei.PrivateUrl(accessId, secretKey, huaweiBucket, endPoint,
                Long.parseLong(expires), getQueriesMap(), savePath);
    }

    private com.qiniu.process.baidu.PrivateUrl getBaiduPrivateUrl(boolean single) throws IOException {
        String accessId = commonParams.getS3AccessId();
        String secretKey = commonParams.getS3SecretKey();
        if (accessId == null || accessId.isEmpty()) {
            accessId = entryParam.getValue("bai-id");
            secretKey = entryParam.getValue("bai-secret");
        }
        String baiduBucket = bucket == null || bucket.isEmpty() ? entryParam.getValue("bucket") : bucket;
        String endPoint = regionName == null || regionName.isEmpty() ? entryParam.getValue("region", regionName) : regionName;
        if (endPoint == null || "".equals(endPoint)) endPoint = CloudApiUtils.getBaiduBosRegion(accessId, secretKey, baiduBucket);
        if (!endPoint.matches("https?://.+")) {
            endPoint = String.join(endPoint, httpsConfigEnabled ? "https://" : "http://", ".bcebos.com");
        }
        String expires = entryParam.getValue("expires", "3600").trim();
        ParamsUtils.checked(expires, "expires", "[1-9]\\d*");
        // 华为 sdk 的过期时间按秒设置
        return single ? new com.qiniu.process.baidu.PrivateUrl(accessId, secretKey, baiduBucket, endPoint, Integer.parseInt(expires),
                getQueriesMap()) : new com.qiniu.process.baidu.PrivateUrl(accessId, secretKey, baiduBucket, endPoint,
                Integer.parseInt(expires), getQueriesMap(), savePath);
    }

    private ILineProcess<Map<String, String>> getDownloadFile(Map<String, String> indexMap, boolean single, boolean useQuery)
            throws IOException {
        String protocol = entryParam.getValue("protocol", "http").trim();
        ParamsUtils.checked(protocol, "protocol", "https?");
        String domain = entryParam.getValue("domain", "").trim();
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        String queries = useQuery ? entryParam.getValue("queries", "").trim() : null;
        String host = entryParam.getValue("host", "").trim();
        String bytes = entryParam.getValue("bytes", "").trim();
        if (bytes.equals("0")) throw new IOException("range bytes can not be 0.");
        int[] range = new int[0];
        if (!"".equals(bytes)) {
            String[] ranges = bytes.split("-");
            if (ranges.length > 2) throw new IOException("range bytes should be like \"0-1024\".");
            try {
                if (ranges.length > 1) {
                    range = new int[2];
                    range[0] = Integer.parseInt(ranges[0]);
                    String byteSize = ranges[1];
                    if (byteSize != null && !"".equals(byteSize)) range[1] = Integer.parseInt(byteSize);
                } else {
                    range = new int[]{Integer.parseInt(ranges[0])};
                }
            } catch (Exception e) {
                throw new IOException("incorrect range bytes value, " + e.toString());
            }
        }
        String preDown = entryParam.getValue("pre-down", "false").trim();
        ParamsUtils.checked(preDown, "pre-down", "(true|false)");
        String addPrefix = entryParam.getValue("add-prefix", null);
        String rmPrefix = entryParam.getValue("rm-prefix", null);
        String downloadPath = entryParam.getValue("down-path", "").trim();
        if (Boolean.parseBoolean(preDown)) {
            downloadPath = null;
        } else if (downloadPath.equals(savePath)) {
            throw new IOException("please change save-path or down-path, because them should not be equal.");
        } else if ("".equals(downloadPath)) {
            if (single) downloadPath = ".";
            else throw new IOException("please set down-path");
        }
        return single ? new DownloadFile(getNewQiniuConfig(), protocol, domain, urlIndex, host, range, queries, addPrefix,
                rmPrefix, downloadPath)
                : new DownloadFile(getNewQiniuConfig(), protocol, domain, urlIndex, host, range, queries, addPrefix, rmPrefix,
                downloadPath, savePath);
    }

    private ILineProcess<Map<String, String>> getImageCensor(Map<String, String> indexMap, boolean single, boolean useQuery)
            throws IOException {
        String protocol = entryParam.getValue("protocol", "http").trim();
        ParamsUtils.checked(protocol, "protocol", "https?");
        String domain = entryParam.getValue("domain", "").trim();
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        String queries = useQuery ? entryParam.getValue("queries", "").trim() : null;
        String[] scenes = entryParam.getValue("scenes").trim().split(",");
        return single ? new ImageCensor(qiniuAccessKey, qiniuSecretKey, getNewQiniuConfig(), protocol, domain, urlIndex,
                queries, scenes) : new ImageCensor(qiniuAccessKey, qiniuSecretKey, getNewQiniuConfig(), protocol, domain,
                urlIndex, queries, scenes, savePath);
    }

    private ILineProcess<Map<String, String>> getVideoCensor(Map<String, String> indexMap, boolean single) throws IOException {
        String protocol = entryParam.getValue("protocol", "http").trim();
        ParamsUtils.checked(protocol, "protocol", "https?");
        String domain = entryParam.getValue("domain", "").trim();
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        String[] scenes = entryParam.getValue("scenes").trim().split(",");
        String interval = entryParam.getValue("interval", "0").trim();
        String saverBucket = entryParam.getValue("save-bucket", "").trim();
        String saverPrefix = entryParam.getValue("saver-prefix", "").trim();
        String hookUrl = entryParam.getValue("callback-url", "").trim();
        String checkUrl = entryParam.getValue("check-url", "true").trim();
        if ("true".equals(checkUrl) && !"".equals(hookUrl)) RequestUtils.checkCallbackUrl(hookUrl);
        return single ? new VideoCensor(qiniuAccessKey, qiniuSecretKey, getNewQiniuConfig(), protocol, domain, urlIndex,
                scenes, Integer.parseInt(interval), saverBucket, saverPrefix, hookUrl) : new VideoCensor(qiniuAccessKey,
                qiniuSecretKey, getNewQiniuConfig(), protocol, domain, urlIndex, scenes, Integer.parseInt(interval),
                saverBucket, saverPrefix, hookUrl, savePath);
    }

    private ILineProcess<Map<String, String>> getCensorResult(Map<String, String> indexMap, boolean single) throws IOException {
        String jobIdIndex = indexMap.containsValue("id") ? "id" : null;
        return single ? new QueryCensorResult(qiniuAccessKey, qiniuSecretKey, getNewQiniuConfig(), jobIdIndex)
                : new QueryCensorResult(qiniuAccessKey, qiniuSecretKey, getNewQiniuConfig(), jobIdIndex, savePath);
    }

    private ILineProcess<Map<String, String>> getQiniuUploadFile(Map<String, String> indexMap, boolean single) throws IOException {
        String pathIndex = indexMap.containsValue("filepath") ? "filepath" : null;
        String parentPath = entryParam.getValue("parent-path", "").trim();
        String recorder = entryParam.getValue("record", "false").trim();
        ParamsUtils.checked(recorder, "record", "(true|false)");
        boolean record = Boolean.parseBoolean(recorder);
        String keepPath = entryParam.getValue("keep-path", "true").trim();
        ParamsUtils.checked(keepPath, "keep-path", "(true|false)");
        boolean keep = Boolean.parseBoolean(keepPath);
        String addPrefix = entryParam.getValue("add-prefix", null);
        String rmPrefix = entryParam.getValue("rm-prefix", null);
        String expiration = entryParam.getValue("expires", "3600").trim();
        long expires = Long.parseLong(expiration);
        StringMap policy = new StringMap();
        StringMap params = new StringMap();
        for (Map.Entry<String, String> entry : entryParam.getParamsMap().entrySet()) {
            if (entry.getKey().matches("policy\\.(deleteAfterDays|isPrefixalScope|insertOnly|fsizeMin|fsizeLimit|" +
                    "detectMime|fileType)")) {
                policy.put(entry.getKey().substring(7), Long.parseLong(entry.getValue().trim()));
            } else if (entry.getKey().startsWith("policy.")) {
                policy.put(entry.getKey().substring(7), entry.getValue().trim());
            } else if (entry.getKey().startsWith("params.")) {
                params.put(entry.getKey().substring(7), entry.getValue().trim());
            }
        }
        String crc = entryParam.getValue("crc", "false").trim();
        ParamsUtils.checked(crc, "crc", "(true|false)");
        boolean checkCrc = Boolean.parseBoolean(crc);
        Configuration configuration = getQiniuConfig();
        String threshold = entryParam.getValue("threshold", "0").trim();
        ParamsUtils.checked(threshold, "threshold", "\\d+");
        int thresholdSize = Integer.parseInt(threshold);
        if (thresholdSize > 4) configuration.putThreshold = thresholdSize * 1024 * 1024;
        return single ? new UploadFile(qiniuAccessKey, qiniuSecretKey, configuration, bucket, pathIndex, parentPath,
                record, keep, addPrefix, rmPrefix, expires, policy, params, checkCrc) : new UploadFile(qiniuAccessKey,
                qiniuSecretKey, configuration, bucket, pathIndex, parentPath, record, keep, addPrefix, rmPrefix,
                expires, policy, params, checkCrc, savePath);
    }

    private ILineProcess<Map<String, String>> getChangeMime(Map<String, String> indexMap, boolean single) throws IOException {
        String mimeIndex = indexMap.containsValue("mime") ? "mime" : null;
        String mimeType = entryParam.getValue("mime", null);
        if (mimeType != null) mimeType = mimeType.trim();
        StringBuilder condition = new StringBuilder();
        for (Map.Entry<String, String> entry : entryParam.getParamsMap().entrySet()) {
            if (entry.getKey().startsWith("cond.")) {
                if (condition.length() > 0) {
                    condition.append(entry.getKey().substring(5)).append("=").append(entry.getValue().trim()).append("&");
                } else {
                    condition.append(entry.getKey().substring(5)).append("=").append(entry.getValue().trim());
                }
            }
        }
        return single ? new ChangeMime(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, mimeType, mimeIndex,
                condition.toString()) : new ChangeMime(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket,
                mimeType, mimeIndex, condition.toString(), savePath);
    }

    private ILineProcess<Map<String, String>> getChangeMetadata(boolean single) throws IOException {
        Map<String, String> metadata = new HashMap<>();
        StringBuilder condition = new StringBuilder();
        for (Map.Entry<String, String> entry : entryParam.getParamsMap().entrySet()) {
            if (entry.getKey().startsWith("meta.")) {
                metadata.put(entry.getKey().substring(5), entry.getValue().trim());
            } else if (entry.getKey().startsWith("cond.")) {
                if (condition.length() > 0) {
                    condition.append(entry.getKey().substring(5)).append("=").append(entry.getValue().trim()).append("&");
                } else {
                    condition.append(entry.getKey().substring(5)).append("=").append(entry.getValue().trim());
                }
            }
        }
        return single ? new ChangeMetadata(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, metadata, condition.toString()) :
                new ChangeMetadata(qiniuAccessKey, qiniuSecretKey, getQiniuConfig(), bucket, metadata, condition.toString(), savePath);
    }

    private ILineProcess<Map<String, String>> getCdnRefresh(Map<String, String> indexMap, boolean single) throws IOException {
        String protocol = entryParam.getValue("protocol", "http").trim();
        ParamsUtils.checked(protocol, "protocol", "https?");
        String domain = entryParam.getValue("domain", "").trim();
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        String dir = entryParam.getValue("is-dir", "false").trim();
        ParamsUtils.checked(dir, "is-dir", "(true|false)");
        boolean isDir = Boolean.parseBoolean(dir);
        return single ? new CdnUrlProcess(qiniuAccessKey, qiniuSecretKey, getNewQiniuConfig(), protocol, domain, urlIndex,
                isDir, false) : new CdnUrlProcess(qiniuAccessKey, qiniuSecretKey, getNewQiniuConfig(), protocol,
                domain, urlIndex, isDir, false, savePath);
    }

    private ILineProcess<Map<String, String>> getCdnPrefetch(Map<String, String> indexMap, boolean single) throws IOException {
        String protocol = entryParam.getValue("protocol", "http").trim();
        ParamsUtils.checked(protocol, "protocol", "https?");
        String domain = entryParam.getValue("domain", "").trim();
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        return single ? new CdnUrlProcess(qiniuAccessKey, qiniuSecretKey, getNewQiniuConfig(), protocol, domain, urlIndex,
                false, true) : new CdnUrlProcess(qiniuAccessKey, qiniuSecretKey, getNewQiniuConfig(), protocol,
                domain, urlIndex, false, true, savePath);
    }

    private ILineProcess<Map<String, String>> getRefreshQuery(Map<String, String> indexMap, boolean single) throws IOException {
        String protocol = entryParam.getValue("protocol", "http").trim();
        ParamsUtils.checked(protocol, "protocol", "https?");
        String domain = entryParam.getValue("domain", "").trim();
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        return single ? new CdnUrlQuery(qiniuAccessKey, qiniuSecretKey, getNewQiniuConfig(), protocol, domain, urlIndex,
                false) : new CdnUrlQuery(qiniuAccessKey, qiniuSecretKey, getNewQiniuConfig(), protocol,
                domain, urlIndex, false, savePath);
    }

    private ILineProcess<Map<String, String>> getPrefetchQuery(Map<String, String> indexMap, boolean single) throws IOException {
        String protocol = entryParam.getValue("protocol", "http").trim();
        ParamsUtils.checked(protocol, "protocol", "https?");
        String domain = entryParam.getValue("domain", "").trim();
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        return single ? new CdnUrlQuery(qiniuAccessKey, qiniuSecretKey, getNewQiniuConfig(), protocol, domain, urlIndex,
                true) : new CdnUrlQuery(qiniuAccessKey, qiniuSecretKey, getNewQiniuConfig(), protocol,
                domain, urlIndex, true, savePath);
    }

    private ILineProcess<Map<String, String>> getFetch(Map<String, String> indexMap, boolean single) throws IOException {
        String ak = qiniuAccessKey == null || qiniuAccessKey.isEmpty() ?
                entryParam.getValue("qiniu-ak").trim() : qiniuAccessKey;
        String sk = qiniuSecretKey == null || qiniuSecretKey.isEmpty() ?
                entryParam.getValue("qiniu-sk").trim() : qiniuSecretKey;
        String toBucket = entryParam.getValue("to-bucket").trim();
        if (toBucket.equals(bucket) && "qiniu".equals(source))
            throw new IOException("the to-bucket can not be same as bucket if source is qiniu.");
        String protocol = entryParam.getValue("protocol", "http").trim();
        ParamsUtils.checked(protocol, "protocol", "https?");
        String domain = entryParam.getValue("domain", "").trim();
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        String addPrefix = entryParam.getValue("add-prefix", null);
        String rmPrefix = entryParam.getValue("rm-prefix", null);
        String regionStr = entryParam.getValue("qiniu-region", regionName).trim();
        Configuration configuration = getDefaultQiniuConfig(ak, sk, regionStr, toBucket);
        return single ? new FetchFile(ak, sk, configuration, toBucket, protocol, domain, urlIndex, addPrefix, rmPrefix)
                : new FetchFile(ak, sk, configuration, toBucket, protocol, domain, urlIndex, addPrefix, rmPrefix, savePath);
    }

    private ILineProcess<Map<String, String>> getSyncUpload(Map<String, String> indexMap, boolean single) throws IOException {
        String ak = qiniuAccessKey == null || qiniuAccessKey.isEmpty() ?
                entryParam.getValue("qiniu-ak").trim() : qiniuAccessKey;
        String sk = qiniuSecretKey == null || qiniuSecretKey.isEmpty() ?
                entryParam.getValue("qiniu-sk").trim() : qiniuSecretKey;
        String toBucket = entryParam.getValue("to-bucket").trim();
        if (toBucket.equals(bucket) && "qiniu".equals(source))
            throw new IOException("the to-bucket can not be same as bucket if source is qiniu.");
        String protocol = entryParam.getValue("protocol", "http").trim();
        ParamsUtils.checked(protocol, "protocol", "https?");
        String domain = entryParam.getValue("domain", "").trim();
        String urlIndex = indexMap.containsValue("url") ? "url" : null;
        String host = entryParam.getValue("host", "").trim();
        String addPrefix = entryParam.getValue("add-prefix", null);
        String rmPrefix = entryParam.getValue("rm-prefix", null);
        String expiration = entryParam.getValue("expires", "3600").trim();
        long expires = Long.parseLong(expiration);
        StringMap policy = new StringMap();
        StringMap params = new StringMap();
        for (Map.Entry<String, String> entry : entryParam.getParamsMap().entrySet()) {
            if (entry.getKey().matches("policy\\.(deleteAfterDays|isPrefixalScope|insertOnly|fsizeMin|fsizeLimit|" +
                    "detectMime|fileType)")) {
                policy.put(entry.getKey().substring(7), Long.parseLong(entry.getValue().trim()));
            } else if (entry.getKey().startsWith("policy.")) {
                policy.put(entry.getKey().substring(7), entry.getValue().trim());
            } else if (entry.getKey().startsWith("params.")) {
                params.put(entry.getKey().substring(7), entry.getValue().trim());
            }
        }
        String regionStr = entryParam.getValue("qiniu-region", regionName).trim();
        Configuration configuration = getDefaultQiniuConfig(ak, sk, regionStr, toBucket);
        return single ? new SyncUpload(ak, sk, configuration, protocol, domain, urlIndex, host, addPrefix, rmPrefix,
                toBucket, expires, policy, params) : new SyncUpload(ak, sk, configuration, protocol, domain, urlIndex,
                host, addPrefix, rmPrefix, toBucket, expires, policy, params, savePath);
    }

    private ILineProcess<Map<String, String>> getDomainsOfBucket(boolean single) throws IOException {
        return single ? new DomainsOfBucket(qiniuAccessKey, qiniuSecretKey, getNewQiniuConfig())
                : new DomainsOfBucket(qiniuAccessKey, qiniuSecretKey, getNewQiniuConfig(), savePath);
    }
}
