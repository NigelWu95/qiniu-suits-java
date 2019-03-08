package com.qiniu.entry;

import com.qiniu.service.filtration.SeniorChecker;
import com.qiniu.service.interfaces.IEntryParam;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.media.QiniuPfop;
import com.qiniu.service.media.QueryAvinfo;
import com.qiniu.service.media.QueryPfopResult;
import com.qiniu.service.filtration.BaseFieldsFilter;
import com.qiniu.service.filtration.FilterProcess;
import com.qiniu.service.qoss.*;
import com.qiniu.storage.Configuration;
import com.qiniu.util.DateUtils;

import java.io.IOException;
import java.util.*;

public class ProcessorChoice {

    private IEntryParam entryParam;
    private CommonParams commonParams;
    private String accessKey;
    private String secretKey;
    private String bucket;
    private Map<String, String> indexMap;
    private String process;
    private int retryCount;
    private String savePath;
    private String saveFormat;
    private String saveSeparator;
    private Configuration configuration;

    public ProcessorChoice(IEntryParam entryParam, Configuration configuration, CommonParams commonParams) {
        this.entryParam = entryParam;
        this.commonParams = commonParams;
        this.accessKey = commonParams.getAccessKey();
        this.secretKey = commonParams.getSecretKey();
        this.indexMap = new HashMap<>();
        if (commonParams.getIndexMap() != null) {
            for (Map.Entry<String, String> entry : commonParams.getIndexMap().entrySet()) {
                this.indexMap.put(entry.getValue(), entry.getKey());
            }
        }
        this.bucket = commonParams.getBucket();
        this.process = commonParams.getProcess();
        this.retryCount = commonParams.getRetryCount();
        this.savePath = commonParams.getSavePath();
        this.saveFormat = commonParams.getSaveFormat();
        this.saveSeparator = commonParams.getSaveSeparator();
        this.configuration = configuration;
    }

    private List<String> getFilterList(String key, String field, String name)
            throws IOException {
        if (!"".equals(field)) {
            if (indexMap == null || indexMap.containsKey(key)) {
                return Arrays.asList(field.split(","));
            } else {
                throw new IOException("f-" + name + " filter must get the " + key + "'s index in indexes settings.");
            }
        } else return null;
    }

    private Long getPointDatetime(String date, String time) throws Exception {
        String pointDatetime;
        if(date.matches("\\d{4}-\\d{2}-\\d{2}")) {
            if (indexMap != null && !indexMap.containsKey("putTime")) {
                throw new IOException("f-date filter must get the putTime's index.");
            }
            if (time.matches("\\d{2}:\\d{2}:\\d{2}"))
                pointDatetime =  date + " " + time;
            else {
                pointDatetime =  date + " " + "00:00:00";
            }
            return DateUtils.parseYYYYMMDDHHMMSSdatetime(pointDatetime);
        } else {
            return 0L;
        }

    }

    public ILineProcess<Map<String, String>> get() throws Exception {
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
        String checkType = entryParam.getValue("f-check", "");
        String date = entryParam.getValue("f-date", "");
        String time = entryParam.getValue("f-time", "");
        String direction = entryParam.getValue("f-direction", "");
        long putTimeMax = 0;
        long putTimeMin = 0;
        if (!"".equals(date)) {
            direction = commonParams.checked(direction, "f-direction", "[01]");
            putTimeMax = "0".equals(direction) ? 0 : getPointDatetime(date, time) * 10000;
            putTimeMin = "1".equals(direction) ? 0 : getPointDatetime(date, time) * 10000;
        }
        String type = entryParam.getValue("type", null);
        String status = entryParam.getValue("status", null);
        if (type != null) type = commonParams.checked(type, "type", "[01]");
        if (status != null) status = commonParams.checked(status, "status", "[01]");

        List<String> keyPrefixList = getFilterList("key", keyPrefix, "prefix");
        List<String> keySuffixList = getFilterList("key", keySuffix, "suffix");
        List<String> keyInnerList = getFilterList("key", keyInner, "inner");
        List<String> keyRegexList = getFilterList("key", keyRegex, "regex");
        List<String> mimeTypeList = getFilterList("mimeType", mimeType, "mime");
        List<String> antiKeyPrefixList = getFilterList("key", antiKeyPrefix, "anti-prefix");
        List<String> antiKeySuffixList = getFilterList("key", antiKeySuffix, "anti-suffix");
        List<String> antiKeyInnerList = getFilterList("key", antiKeyInner, "anti-inner");
        List<String> antiKeyRegexList = getFilterList("key", antiKeyRegex, "anti-regex");
        List<String> antiMimeTypeList = getFilterList("mimeType", antiMimeType, "anti-mime");
        BaseFieldsFilter baseFieldsFilter = new BaseFieldsFilter();
        baseFieldsFilter.setKeyConditions(keyPrefixList, keySuffixList, keyInnerList, keyRegexList);
        baseFieldsFilter.setAntiKeyConditions(antiKeyPrefixList, antiKeySuffixList, antiKeyInnerList, antiKeyRegexList);
        baseFieldsFilter.setMimeTypeConditions(mimeTypeList, antiMimeTypeList);
        baseFieldsFilter.setOtherConditions(putTimeMax, putTimeMin, type, status);
        SeniorChecker seniorChecker = new SeniorChecker(checkType);

        ILineProcess<Map<String, String>> processor;
        ILineProcess<Map<String, String>> nextProcessor = process == null ? null : whichNextProcessor();
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
        if (processor != null) processor.setRetryCount(retryCount);
        return processor;
    }

    private ILineProcess<Map<String, String>> whichNextProcessor() throws Exception {
        ILineProcess<Map<String, String>> processor = null;
        switch (process) {
            case "status": processor = getChangeStatus(); break;
            case "type": processor = getChangeType(); break;
            case "lifecycle": processor = getUpdateLifecycle(); break;
            case "copy": processor = getCopyFile(); break;
            case "move":
            case "rename": processor = getMoveFile(); break;
            case "delete": processor = getDeleteFile(); break;
            case "asyncfetch": processor = getAsyncFetch(); break;
            case "avinfo": processor = getQueryAvinfo(); break;
            case "pfop": processor = getQiniuPfop(); break;
            case "pfopresult": processor = getPfopResult(); break;
            case "qhash": processor = getQueryHash(); break;
            case "stat": processor = getFileStat(); break;
            case "privateurl": processor = getPrivateUrl(); break;
        }
        if (processor != null) processor.setRetryCount(retryCount);
        return processor;
    }

    private ILineProcess<Map<String, String>> getChangeStatus() throws IOException {
        String status = commonParams.checked(entryParam.getValue("status"), "status", "[01]");
        return new ChangeStatus(accessKey, secretKey, configuration, bucket, Integer.valueOf(status), savePath);
    }

    private ILineProcess<Map<String, String>> getChangeType() throws IOException {
        String type = commonParams.checked(entryParam.getValue("type"), "type", "[01]");
        return new ChangeType(accessKey, secretKey, configuration, bucket, Integer.valueOf(type), savePath);
    }

    private ILineProcess<Map<String, String>> getUpdateLifecycle() throws IOException {
        String days = commonParams.checked(entryParam.getValue("days"), "days", "[01]");
        return new UpdateLifecycle(accessKey, secretKey, configuration, bucket, Integer.valueOf(days), savePath);
    }

    private ILineProcess<Map<String, String>> getCopyFile() throws IOException {
        String toBucket = entryParam.getValue("to-bucket");
        String newKeyIndex = indexMap.get("newKey");
        String addPrefix = entryParam.getValue("add-prefix", null);
        String rmPrefix = entryParam.getValue("rm-prefix", null);
        return new CopyFile(accessKey, secretKey, configuration, bucket, toBucket, newKeyIndex, addPrefix, rmPrefix, savePath);
    }

    private ILineProcess<Map<String, String>> getMoveFile() throws IOException {
        String toBucket = entryParam.getValue("to-bucket", null);
        if ("move".equals(process) && toBucket == null) throw new IOException("no incorrect to-bucket, please set it.");
        String newKeyIndex = indexMap.get("newKey");
        String addPrefix = entryParam.getValue("add-prefix", null);
        String rmPrefix = entryParam.getValue("rm-prefix", null);
        String force = entryParam.getValue("prefix-force", null);
        force = commonParams.checked(force, "prefix-force", "(true|false)");
        return new MoveFile(accessKey, secretKey, configuration, bucket, toBucket, newKeyIndex, addPrefix, rmPrefix,
                Boolean.valueOf(force), savePath);
    }

    private ILineProcess<Map<String, String>> getDeleteFile() throws IOException {
        return new DeleteFile(accessKey, secretKey, configuration, bucket, savePath);
    }

    private ILineProcess<Map<String, String>> getAsyncFetch() throws IOException {
        String toBucket = entryParam.getValue("to-bucket");
        String domain = entryParam.getValue("domain", null);
        String protocol = entryParam.getValue("protocol", "http");
        protocol = commonParams.checked(protocol, "protocol", "https?");
        String sign = entryParam.getValue("private", "false");
        sign = commonParams.checked(sign, "private", "(true|false)");
        String keyPrefix = entryParam.getValue("add-prefix", null);
        String urlIndex = indexMap.get("url");
        String host = entryParam.getValue("host", null);
        String md5Index = indexMap.get("md5");
        String callbackUrl = entryParam.getValue("callback-url", null);
        String callbackBody = entryParam.getValue("callback-body", null);
        String callbackBodyType = entryParam.getValue("callback-body-type", null);
        String callbackHost = entryParam.getValue("callback-host", null);
        String type = entryParam.getValue("file-type", "0");
        String ignore = entryParam.getValue("ignore-same-key", "false");
        ignore = commonParams.checked(ignore, "ignore-same-key", "(true|false)");
        ILineProcess<Map<String, String>> processor = new AsyncFetch(accessKey, secretKey, configuration, toBucket,
                domain, protocol, Boolean.valueOf(sign), keyPrefix, urlIndex, savePath);
        if (host != null || md5Index != null || callbackUrl != null || callbackBody != null || callbackBodyType != null
                || callbackHost != null || "1".equals(type) || "true".equals(ignore)) {
            ((AsyncFetch) processor).setFetchArgs(host, md5Index, callbackUrl, callbackBody,
                    callbackBodyType, callbackHost, Integer.valueOf(type), Boolean.valueOf(ignore));
        };
        return processor;
    }

    private ILineProcess<Map<String, String>> getQueryAvinfo() throws IOException {
        String domain = entryParam.getValue("domain");
        String protocol = entryParam.getValue("protocol", "http");
        protocol = commonParams.checked(protocol, "protocol", "https?");
        String urlIndex = indexMap.get("url");
        String sign = entryParam.getValue("private", "false");
        sign = commonParams.checked(sign, "private", "(true|false)");
        if (Boolean.valueOf(sign) && accessKey == null) {
            accessKey = entryParam.getValue("ak");
            secretKey = entryParam.getValue("sk");
        }
        return new QueryAvinfo(domain, protocol, urlIndex, accessKey, secretKey, savePath);
    }

    private ILineProcess<Map<String, String>> getQiniuPfop() throws IOException {
        String fopsIndex = indexMap.get("fops");
        String forcePublic = entryParam.getValue("force-public", "false");
        String pipeline = entryParam.getValue("pipeline", null);
        if (pipeline == null && !"true".equals(forcePublic)) {
            throw new IOException("please set pipeline, if you don't want to use" +
                    " private pipeline, please set the force-public as true.");
        }
        return new QiniuPfop(accessKey, secretKey, configuration, bucket, pipeline, fopsIndex, savePath);
    }

    private ILineProcess<Map<String, String>> getPfopResult() throws IOException {
        String persistentIdIndex = indexMap.get("pid");
        return new QueryPfopResult(persistentIdIndex, savePath);
    }

    private ILineProcess<Map<String, String>> getQueryHash() throws IOException {
        String domain = entryParam.getValue("domain");
        String algorithm = entryParam.getValue("algorithm", "md5");
        algorithm = commonParams.checked(algorithm, "algorithm", "(md5|sha1)");
        String protocol = entryParam.getValue("protocol", "http");
        protocol = commonParams.checked(protocol, "protocol", "https?");
        String urlIndex = indexMap.get("url");
        String sign = entryParam.getValue("private", "false");
        sign = commonParams.checked(sign, "private", "(true|false)");
        if (Boolean.valueOf(sign)) {
            accessKey = entryParam.getValue("ak");
            secretKey = entryParam.getValue("sk");
        }
        return new QueryHash(domain, algorithm, protocol, urlIndex, accessKey, secretKey, savePath);
    }

    private ILineProcess<Map<String, String>> getFileStat() throws IOException {
        return new FileStat(accessKey, secretKey, configuration, bucket, savePath, saveFormat, saveSeparator);
    }

    private ILineProcess<Map<String, String>> getPrivateUrl() throws IOException {
        String domain = entryParam.getValue("domain");
        String protocol = entryParam.getValue("protocol", "http");
        protocol = commonParams.checked(protocol, "protocol", "https?");
        String urlIndex = indexMap.get("url");
        String expires = entryParam.getValue("expires", "3600");
        expires = commonParams.checked(expires, "expires", "[1-9]\\d*");
        return new PrivateUrl(accessKey, secretKey, domain, protocol, urlIndex, Long.valueOf(expires), savePath);
    }
}
