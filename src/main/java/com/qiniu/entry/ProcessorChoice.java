package com.qiniu.entry;

import com.qiniu.service.filtration.SeniorChecker;
import com.qiniu.service.interfaces.IEntryParam;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.media.PfopCommand;
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
                return commonParams.splitItems(field);
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

    private Long checkedDatetime(String datetime) throws Exception {
        long time;
        if (datetime == null ||datetime.matches("(|0)")) {
            time = 0L;
        } else if (datetime.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")) {
            time = DateUtils.parseYYYYMMDDHHMMSSdatetime(datetime);
        } else if (datetime.matches("\\d{4}-\\d{2}-\\d{2}")) {
            time = DateUtils.parseYYYYMMDDHHMMSSdatetime(datetime + " 00:00:00");
        } else {
            throw new IOException("please check your datetime string format, set it as \"yyyy-MM-dd HH:mm:ss\".");
        }
        if (time > 0L && indexMap != null && !indexMap.containsKey("putTime")) {
            throw new IOException("f-date filter must get the putTime's index.");
        }
        return time * 10000;
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
        String[] dateScale = splitDateScale(entryParam.getValue("f-date-scale", null));
        long putTimeMin = checkedDatetime(dateScale[0]);
        long putTimeMax = checkedDatetime(dateScale[1]);
        if (putTimeMax != 0 && putTimeMax <= putTimeMin ) {
            throw new IOException("please set date scale to make first as start date, next as end date, <date1> " +
                    "should earlier then <date2>.");
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
        baseFieldsFilter.setOtherConditions(putTimeMin, putTimeMax, type, status);

        String checkType = entryParam.getValue("f-check", "");
        checkType = commonParams.checked(checkType, "f-check", "(|mime)");
        String checkConfig = entryParam.getValue("f-check-config", "");
        String checkRewrite = entryParam.getValue("f-check-rewrite", "false");
        checkRewrite = commonParams.checked(checkRewrite, "f-check-rewrite", "(true|false)");
        SeniorChecker seniorChecker = new SeniorChecker(checkType, checkConfig, Boolean.valueOf(checkRewrite));

        ILineProcess<Map<String, String>> processor;
        ILineProcess<Map<String, String>> nextProcessor = process == null ? null : whichNextProcessor();
        // 为了保证程序出现因网络等原因产生的非预期异常时正常运行需要设置重试次数，filter 操作不需要重试
        if (nextProcessor != null) nextProcessor.setRetryCount(retryCount);
        if (baseFieldsFilter.isValid() || seniorChecker.isValid()) {
            // 如果设置了 filter，默认情况下不保留原始数据
            commonParams.setSaveTotal(false);
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
            case "pfopcmd": processor = getPfopCommand(); break;
            case "mirror": processor = getMirrorFetch(); break;
        }
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

    private ILineProcess<Map<String, String>> getPfopCommand() throws IOException {
        String configJson = entryParam.getValue("pfop-config");
        String duration = entryParam.getValue("duration", "false");
        duration = commonParams.checked(duration, "duration", "(true|false)");
        String size = entryParam.getValue("size", "false");
        size = commonParams.checked(size, "size", "(true|false)");
        return new PfopCommand(configJson, Boolean.valueOf(duration), Boolean.valueOf(size), savePath);
    }

    private ILineProcess<Map<String, String>> getMirrorFetch() throws IOException {
        return new MirrorFetch(accessKey, secretKey, configuration, bucket, savePath);
    }
}
