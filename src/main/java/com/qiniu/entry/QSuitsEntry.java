package com.qiniu.entry;

import com.qiniu.common.Zone;
import com.qiniu.config.ParamsConfig;
import com.qiniu.datasource.BucketList;
import com.qiniu.datasource.FileInput;
import com.qiniu.datasource.IDataSource;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.process.filtration.BaseFieldsFilter;
import com.qiniu.process.filtration.FilterProcess;
import com.qiniu.process.filtration.SeniorChecker;
import com.qiniu.process.qdora.PfopCommand;
import com.qiniu.process.qdora.QiniuPfop;
import com.qiniu.process.qdora.QueryAvinfo;
import com.qiniu.process.qdora.QueryPfopResult;
import com.qiniu.process.qoss.*;
import com.qiniu.storage.Configuration;
import com.qiniu.util.ProcessUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class QSuitsEntry {

    private IEntryParam entryParam;
    private CommonParams commonParams;
    private Configuration configuration;
    private String source;
    private String accessKey;
    private String secretKey;
    private String bucket;
    private int unitLen;
    private int threads;
    private boolean saveTotal;
    private List<String> rmFields;
    private String process;
    private String rmPrefix;
    private int batchSize;
    private int retryTimes;
    private String savePath;
    private String saveFormat;
    private String saveSeparator;

    public QSuitsEntry(String[] args) throws IOException {
        setEntryParam(args);
        this.commonParams = new CommonParams(entryParam);
        setConfiguration();
        setMembers();
    }

    public QSuitsEntry(IEntryParam entryParam) throws IOException {
        this.entryParam = entryParam;
        this.commonParams = new CommonParams(entryParam);
        setMembers();
    }

    public QSuitsEntry(IEntryParam entryParam, Configuration configuration) throws IOException {
        this.entryParam = entryParam;
        this.configuration = configuration;
        this.commonParams = new CommonParams(entryParam);
        setMembers();
    }

    public void UpdateEntry(IEntryParam entryParam) throws IOException {
        this.entryParam = entryParam;
        this.commonParams = new CommonParams(entryParam);
        setConfiguration();
        setMembers();
    }

    public void UpdateEntry(CommonParams commonParams) {
        this.commonParams = commonParams;
        setMembers();
    }

    public void UpdateEntry(Configuration configuration) {
        this.configuration = configuration;
    }

    private void setMembers() {
        this.source = commonParams.getSource();
        this.accessKey = commonParams.getAccessKey();
        this.secretKey = commonParams.getSecretKey();
        this.bucket = commonParams.getBucket();
        this.unitLen = commonParams.getUnitLen();
        this.threads = commonParams.getThreads();
        this.saveTotal = commonParams.getSaveTotal();
        this.rmFields = commonParams.getRmFields();
        this.process = commonParams.getProcess();
        this.rmPrefix = commonParams.getRmPrefix();
        this.batchSize = commonParams.getBatchSize();
        this.retryTimes = commonParams.getRetryTimes();
        this.savePath = commonParams.getSavePath() + commonParams.getSaveTag();
        this.saveFormat = commonParams.getSaveFormat();
        this.saveSeparator = commonParams.getSaveSeparator();
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

    public IEntryParam getEntryParam() {
        return entryParam;
    }

    public CommonParams getCommonParams() {
        return commonParams;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    private void setConfiguration() {
        this.configuration = new Configuration(Zone.autoZone());
        // 自定义超时时间
        configuration.connectTimeout = Integer.valueOf(entryParam.getValue("connect-timeout", "60"));
        configuration.readTimeout = Integer.valueOf(entryParam.getValue("read-timeout", "120"));
        configuration.writeTimeout = Integer.valueOf(entryParam.getValue("write-timeout", "60"));
    }

    public IDataSource getDataSource() {
        if ("list".equals(source)) {
            return getBucketList();
        } else if ("file".equals(source)) {
            return getFileInput();
        } else {
            return null;
        }
    }

    public FileInput getFileInput() {
        String filePath = commonParams.getPath();
        String parseType = commonParams.getParse();
        String separator = commonParams.getSeparator();
        HashMap<String, String> indexMap = commonParams.getIndexMap();
        FileInput fileInput = new FileInput(filePath, parseType, separator, indexMap, unitLen, threads, savePath);
        fileInput.setSaveOptions(saveTotal, saveFormat, saveSeparator, rmFields);
        fileInput.setRetryTimes(retryTimes);
        return fileInput;
    }

    public BucketList getBucketList() {
        Map<String, String[]> prefixesMap = commonParams.getPrefixesMap();
        List<String> antiPrefixes = commonParams.getAntiPrefixes();
        boolean prefixLeft = commonParams.getPrefixLeft();
        boolean prefixRight = commonParams.getPrefixRight();
        BucketList bucketList = new BucketList(accessKey, secretKey, configuration, bucket, unitLen, prefixesMap,
                antiPrefixes, prefixLeft, prefixRight, threads, savePath);
        bucketList.setSaveOptions(saveTotal, saveFormat, saveSeparator, rmFields);
        bucketList.setRetryTimes(retryTimes);
        return bucketList;
    }

    private String[] splitDateScale(String dateScale) throws IOException {
        String[] scale;
        if (dateScale != null && !"".equals(dateScale)) {
            // 设置的 dateScale 格式应该为 [yyyy-MM-dd HH:mm:ss,yyyy-MM-dd HH:mm:ss]
            if (dateScale.startsWith("[") && dateScale.endsWith("]")) {
                scale = dateScale.substring(1, dateScale.length() - 1).split(",");
            } else if (dateScale.startsWith("[") || dateScale.endsWith("]")) {
                throw new IOException("please check your date scale, set it as \"[<date1>,<date2>]\".");
            } else {
                scale = dateScale.split(",");
            }
        } else {
            scale = new String[]{"", ""};
        }
        if (scale.length <= 1) {
            throw new IOException("please set start and end date, if no start please set is as \"[0,<date>]\"");
        }
        return scale;
    }

    public ILineProcess<Map<String, String>> getProcessor() throws Exception {
        String keyPrefix = entryParam.getValue("f-prefix", "");
        String keySuffix = entryParam.getValue("f-suffix", "");
        String keyInner = entryParam.getValue("f-inner", "");
        String keyRegex = entryParam.getValue("f-regex", "");
        String mimeType = entryParam.getValue("f-mime", "");
        String antiKeyPrefix = entryParam.getValue("f-anti-prefix", "");
        String antiKeySuffix = entryParam.getValue("f-anti-suffix", "");
        String antiKeyInner = entryParam.getValue("f-anti-inner", "");
        String antiKeyRegex = entryParam.getValue("f-anti-regex", "");
        String antiMimeType = entryParam.getValue("f-anti-mime", "");
        String[] dateScale = splitDateScale(entryParam.getValue("f-date-scale", null));
        long putTimeMin = commonParams.checkedDatetime(dateScale[0]);
        long putTimeMax = commonParams.checkedDatetime(dateScale[1]);
        if (putTimeMax != 0 && putTimeMax <= putTimeMin ) {
            throw new IOException("please set date scale to make first as start date, next as end date, <date1> " +
                    "should earlier then <date2>.");
        }
        String type = entryParam.getValue("f-type", null);
        String status = entryParam.getValue("f-status", null);
        if (type != null) type = commonParams.checked(type, "f-type", "[01]");
        if (status != null) status = commonParams.checked(status, "f-status", "[01]");

        List<String> keyPrefixList = commonParams.getFilterList("key", keyPrefix, "prefix");
        List<String> keySuffixList = commonParams.getFilterList("key", keySuffix, "suffix");
        List<String> keyInnerList = commonParams.getFilterList("key", keyInner, "inner");
        List<String> keyRegexList = commonParams.getFilterList("key", keyRegex, "regex");
        List<String> mimeTypeList = commonParams.getFilterList("mimeType", mimeType, "mime");
        List<String> antiKeyPrefixList = commonParams.getFilterList("key", antiKeyPrefix, "anti-prefix");
        List<String> antiKeySuffixList = commonParams.getFilterList("key", antiKeySuffix, "anti-suffix");
        List<String> antiKeyInnerList = commonParams.getFilterList("key", antiKeyInner, "anti-inner");
        List<String> antiKeyRegexList = commonParams.getFilterList("key", antiKeyRegex, "anti-regex");
        List<String> antiMimeTypeList = commonParams.getFilterList("mimeType", antiMimeType, "anti-mime");
        BaseFieldsFilter baseFieldsFilter = new BaseFieldsFilter();
        baseFieldsFilter.setKeyConditions(keyPrefixList, keySuffixList, keyInnerList, keyRegexList);
        baseFieldsFilter.setAntiKeyConditions(antiKeyPrefixList, antiKeySuffixList, antiKeyInnerList, antiKeyRegexList);
        baseFieldsFilter.setMimeTypeConditions(mimeTypeList, antiMimeTypeList);
        baseFieldsFilter.setOtherConditions(putTimeMin, putTimeMax, type, status);

        String checkType = entryParam.getValue("f-check", "");
        checkType = commonParams.checked(checkType, "f-check", "(|mime)");
        String checkConfig = entryParam.getValue("f-check-config", "");
        String checkRewrite = entryParam.getValue("f-check-rewrite", "false");
        checkRewrite = commonParams.checked(checkRewrite, "f-check-rewrite", "(true|false)");
        SeniorChecker seniorChecker = new SeniorChecker(checkType, checkConfig, Boolean.valueOf(checkRewrite));

        ILineProcess<Map<String, String>> processor;
        ILineProcess<Map<String, String>> nextProcessor = process == null ? null : whichNextProcessor();
        if (nextProcessor != null) {
            if (ProcessUtils.canBatch(nextProcessor.getProcessName())) nextProcessor.setBatchSize(batchSize);
            // 为了保证程序出现因网络等原因产生的非预期异常时正常运行需要设置重试次数，filter 操作不需要重试
            nextProcessor.setRetryTimes(retryTimes);
        }
        if (baseFieldsFilter.isValid() || seniorChecker.isValid()) {
            List<String> rmFields = Arrays.asList(entryParam.getValue("rm-fields", "").split(","));
            processor = new FilterProcess(baseFieldsFilter, seniorChecker, savePath, saveFormat, saveSeparator, rmFields);
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
            case "pfop": processor = getPfop(); break;
            case "pfopresult": processor = getPfopResult(); break;
            case "qhash": processor = getQueryHash(); break;
            case "stat": processor = getStatFile(); break;
            case "privateurl": processor = getPrivateUrl(); break;
            case "pfopcmd": processor = getPfopCommand(); break;
            case "mirror": processor = getMirrorFile(); break;
            case "exportts": processor = getExportTs(); break;
        }
        return processor;
    }

    private ILineProcess<Map<String, String>> getChangeStatus() throws IOException {
        String status = commonParams.checked(entryParam.getValue("status"), "status", "[01]");
        return new ChangeStatus(accessKey, secretKey, configuration, bucket, Integer.valueOf(status), rmPrefix, savePath);
    }

    private ILineProcess<Map<String, String>> getChangeType() throws IOException {
        String type = commonParams.checked(entryParam.getValue("type"), "type", "[01]");
        return new ChangeType(accessKey, secretKey, configuration, bucket, Integer.valueOf(type), rmPrefix, savePath);
    }

    private ILineProcess<Map<String, String>> getChangeLifecycle() throws IOException {
        String days = commonParams.checked(entryParam.getValue("days"), "days", "[01]");
        return new ChangeLifecycle(accessKey, secretKey, configuration, bucket, Integer.valueOf(days), rmPrefix, savePath);
    }

    private ILineProcess<Map<String, String>> getCopyFile() throws IOException {
        String toBucket = entryParam.getValue("to-bucket");
        String newKeyIndex = commonParams.containIndex("newKey") ? "newKey" : null;
        String addPrefix = entryParam.getValue("add-prefix", null);
        return new CopyFile(accessKey, secretKey, configuration, bucket, toBucket, newKeyIndex, addPrefix, rmPrefix, savePath);
    }

    private ILineProcess<Map<String, String>> getMoveFile() throws IOException {
        String toBucket = entryParam.getValue("to-bucket", null);
        if ("move".equals(process) && toBucket == null) throw new IOException("no incorrect to-bucket, please set it.");
        String newKeyIndex = commonParams.containIndex("newKey") ? "newKey" : null;
        String addPrefix = entryParam.getValue("add-prefix", null);
        String force = entryParam.getValue("prefix-force", null);
        force = commonParams.checked(force, "prefix-force", "(true|false)");
        return new MoveFile(accessKey, secretKey, configuration, bucket, toBucket, newKeyIndex, addPrefix,
                Boolean.valueOf(force), rmPrefix, savePath);
    }

    private ILineProcess<Map<String, String>> getDeleteFile() throws IOException {
        return new DeleteFile(accessKey, secretKey, configuration, bucket, rmPrefix, savePath);
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
        ILineProcess<Map<String, String>> processor = new AsyncFetch(accessKey, secretKey, configuration, toBucket,
                domain, protocol, urlIndex, addPrefix, rmPrefix, savePath);
        if (host != null || md5Index != null || callbackUrl != null || callbackBody != null || callbackBodyType != null
                || callbackHost != null || "1".equals(type) || "true".equals(ignore)) {
            ((AsyncFetch) processor).setFetchArgs(host, md5Index, callbackUrl, callbackBody,
                    callbackBodyType, callbackHost, Integer.valueOf(type), Boolean.valueOf(ignore));
        }
        return processor;
    }

    private ILineProcess<Map<String, String>> getQueryAvinfo() throws IOException {
        String domain = entryParam.getValue("domain");
        String protocol = entryParam.getValue("protocol", "http");
        protocol = commonParams.checked(protocol, "protocol", "https?");
        String urlIndex = commonParams.containIndex("url") ? "url" : null;
        return new QueryAvinfo(domain, protocol, urlIndex, rmPrefix, savePath);
    }

    private ILineProcess<Map<String, String>> getPfop() throws IOException {
        String fopsIndex = commonParams.containIndex("fops") ? "fops" : null;
        String forcePublic = entryParam.getValue("force-public", "false");
        String pipeline = entryParam.getValue("pipeline", null);
        if (pipeline == null && !"true".equals(forcePublic)) {
            throw new IOException("please set pipeline, if you don't want to use" +
                    " private pipeline, please set the force-public as true.");
        }
        return new QiniuPfop(accessKey, secretKey, configuration, bucket, pipeline, fopsIndex, rmPrefix, savePath);
    }

    private ILineProcess<Map<String, String>> getPfopResult() throws IOException {
        String persistentIdIndex = commonParams.containIndex("pid") ? "pid" : null;
        return new QueryPfopResult(persistentIdIndex, savePath);
    }

    private ILineProcess<Map<String, String>> getQueryHash() throws IOException {
        String domain = entryParam.getValue("domain");
        String algorithm = entryParam.getValue("algorithm", "md5");
        algorithm = commonParams.checked(algorithm, "algorithm", "(md5|sha1)");
        String protocol = entryParam.getValue("protocol", "http");
        protocol = commonParams.checked(protocol, "protocol", "https?");
        String urlIndex = commonParams.containIndex("url") ? "url" : null;
        return new QueryHash(algorithm, protocol, domain, urlIndex, rmPrefix, savePath);
    }

    private ILineProcess<Map<String, String>> getStatFile() throws IOException {
        String rmPrefix = entryParam.getValue("rm-prefix", null);
        return new StatFile(accessKey, secretKey, configuration, bucket, rmPrefix, savePath, saveFormat, saveSeparator);
    }

    private ILineProcess<Map<String, String>> getPrivateUrl() throws IOException {
        String domain = entryParam.getValue("domain");
        String protocol = entryParam.getValue("protocol", "http");
        protocol = commonParams.checked(protocol, "protocol", "https?");
        String urlIndex = commonParams.containIndex("url") ? "url" : null;
        String expires = entryParam.getValue("expires", "3600");
        expires = commonParams.checked(expires, "expires", "[1-9]\\d*");
        return new PrivateUrl(accessKey, secretKey, domain, protocol, urlIndex, Long.valueOf(expires), rmPrefix, savePath);
    }

    private ILineProcess<Map<String, String>> getPfopCommand() throws IOException {
        String configJson = entryParam.getValue("pfop-config");
        String duration = entryParam.getValue("duration", "false");
        duration = commonParams.checked(duration, "duration", "(true|false)");
        String size = entryParam.getValue("size", "false");
        size = commonParams.checked(size, "size", "(true|false)");
        String avinfoIndex = commonParams.containIndex("avinfo") ? "avinfo" : null;
        return new PfopCommand(configJson, Boolean.valueOf(duration), Boolean.valueOf(size), avinfoIndex, rmPrefix, savePath);
    }

    private ILineProcess<Map<String, String>> getMirrorFile() throws IOException {
        return new MirrorFile(accessKey, secretKey, configuration, bucket, rmPrefix, savePath);
    }

    private ILineProcess<Map<String, String>> getExportTs() throws IOException {
        String domain = entryParam.getValue("domain");
        String protocol = entryParam.getValue("protocol", "http");
        protocol = commonParams.checked(protocol, "protocol", "https?");
        String urlIndex = commonParams.containIndex("url") ? "url" : null;
        return new ExportTS(configuration, domain, protocol, urlIndex, rmPrefix, savePath);
    }
}
