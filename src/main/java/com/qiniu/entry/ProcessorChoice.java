package com.qiniu.entry;

import com.qiniu.model.parameter.*;
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
import com.qiniu.util.Auth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ProcessorChoice {

    private IEntryParam entryParam;
    private String process;
    private int retryCount;
    private String resultPath;
    private String resultFormat;
    private String resultSeparator;
    private Configuration configuration;
    final private List<String> needAuthProcesses = new ArrayList<String>(){{
        add("status");
        add("type");
        add("lifecycle");
        add("copy");
        add("move");
        add("rename");
        add("delete");
        add("asyncfetch");
        add("pfop");
        add("stat");
        add("privateurl");
    }};
    private Auth auth;

    public ProcessorChoice(IEntryParam entryParam, Configuration configuration) throws IOException {
        this.entryParam = entryParam;
        FileInputParams fileInputParams = new FileInputParams(entryParam);
        this.process = fileInputParams.getProcess();
        this.retryCount = fileInputParams.getRetryCount();
        this.resultPath = fileInputParams.getResultPath();
        this.resultFormat = fileInputParams.getResultFormat();
        this.resultSeparator = fileInputParams.getResultSeparator();
        this.configuration = configuration;
    }

    public ILineProcess<Map<String, String>> getFileProcessor() throws Exception {
        FileFilterParams fileFilterParams = new FileFilterParams(entryParam);
        BaseFieldsFilter baseFieldsFilter = new BaseFieldsFilter();
        SeniorChecker seniorChecker = new SeniorChecker(fileFilterParams.getCheckType());
        baseFieldsFilter.setKeyConditions(fileFilterParams.getKeyPrefix(), fileFilterParams.getKeySuffix(),
                fileFilterParams.getKeyInner(), fileFilterParams.getKeyRegex());
        baseFieldsFilter.setAntiKeyConditions(fileFilterParams.getAntiKeyPrefix(), fileFilterParams.getAntiKeySuffix(),
                fileFilterParams.getAntiKeyInner(), fileFilterParams.getAntiKeyRegex());
        baseFieldsFilter.setMimeTypeConditions(fileFilterParams.getMimeType(), fileFilterParams.getAntiMimeType());
        baseFieldsFilter.setOtherConditions(fileFilterParams.getPutTimeMax(), fileFilterParams.getPutTimeMin(),
                fileFilterParams.getType(), fileFilterParams.getStatus());
        ILineProcess<Map<String, String>> processor;
        ILineProcess<Map<String, String>> nextProcessor = whichNextProcessor();
        if (baseFieldsFilter.isValid() || seniorChecker.isValid()) {
            processor = new FilterProcess(baseFieldsFilter, seniorChecker, resultPath, resultFormat, resultSeparator,
                    fileFilterParams.getRmFields());
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
        if (needAuthProcesses.contains(process)) {
            String accessKey = entryParam.getValue("ak");
            String secretKey = entryParam.getValue("sk");
            auth = Auth.create(accessKey, secretKey);
        }
        ILineProcess<Map<String, String>> processor = null;
        switch (process) {
            case "status": {
                processor = getChangeStatus();
                break;
            }
            case "type": {
                processor = getChangeType();
                break;
            }
            case "lifecycle": {
                processor = getUpdateLifecycle();
                break;
            }
            case "copy": {
                processor = getCopyFile();
                break;
            }
            case "move":
            case "rename": {
                processor = getMoveFile();
                break;
            }
            case "delete": {
                processor = getDeleteFile();
                break;
            }
            case "asyncfetch": {
                processor = getAsyncFetch();
                break;
            }
            case "avinfo": {
                processor = getQueryAvinfo();
                break;
            }
            case "pfop": {
                processor = getQiniuPfop();
                break;
            }
            case "pfopresult": {
                processor = getPfopResult();
                break;
            }
            case "qhash": {
                processor = getQueryHash();
                break;
            }
            case "stat": {
                processor = getFileStat();
                break;
            }
            case "privateurl": {
                processor = getPrivateUrl();
                break;
            }
        }
        if (processor != null) processor.setRetryCount(retryCount);
        return processor;
    }

    /**
     * 检测值是否满足正则表达式
     * @param value 原值
     * @param conditionRegex 正则表达式
     * @return
     */
    private boolean checkValue(String value, String conditionRegex) {
        if (value == null) return false;
        else return value.matches(conditionRegex);
    }

    private ILineProcess<Map<String, String>> getChangeStatus() throws IOException {
        String bucket = entryParam.getValue("bucket");
        String status = entryParam.getValue("status");
        if (!checkValue(status, "[01]")) {
            throw new IOException("no incorrect status, please set it 0 or 1");
        }
        return new ChangeStatus(auth, configuration, bucket, Integer.valueOf(status), resultPath);
    }

    private ILineProcess<Map<String, String>> getChangeType() throws IOException {
        String bucket = entryParam.getValue("bucket");
        String type = entryParam.getValue("type");
        if (!checkValue(type, "[01]")) {
            throw new IOException("no incorrect type, please set it 0 or 1");
        }
        return new ChangeType(auth, configuration, bucket, Integer.valueOf(type), resultPath);
    }

    private ILineProcess<Map<String, String>> getUpdateLifecycle() throws IOException {
        String bucket = entryParam.getValue("bucket");
        String days = entryParam.getValue("days");
        if (!checkValue(days, "[\\d]+")) {
            throw new IOException("no incorrect days, please set it 0 or 1");
        }
        return new UpdateLifecycle(auth, configuration, bucket, Integer.valueOf(days), resultPath);
    }

    private ILineProcess<Map<String, String>> getCopyFile() throws IOException {
        String bucket = entryParam.getValue("bucket");
        String toBucket = entryParam.getValue("to-bucket");
        String newKeyIndex = entryParam.getValue("newKey-index", null);
        String addPrefix = entryParam.getValue("add-prefix", null);
        String rmPrefix = entryParam.getValue("rm-prefix", null);
        return new CopyFile(auth, configuration, bucket, toBucket, newKeyIndex, addPrefix, rmPrefix, resultPath);
    }

    private ILineProcess<Map<String, String>> getMoveFile() throws IOException {
        String bucket = entryParam.getValue("bucket");
        String toBucket = entryParam.getValue("to-bucket", null);
        if ("move".equals(process) && toBucket == null) throw new IOException("no incorrect to-bucket, please set it.");
        String newKeyIndex = entryParam.getValue("newKey-index", null);
        String addPrefix = entryParam.getValue("add-prefix", null);
        String rmPrefix = entryParam.getValue("rm-prefix", null);
        String forceIfOnlyPrefix = entryParam.getValue("prefix-force", null);
        if (!checkValue(forceIfOnlyPrefix, "(true|false)")) {
            throw new IOException("no incorrect prefix-force, please set it true or false");
        }
        return new MoveFile(auth, configuration, bucket, toBucket, newKeyIndex, addPrefix, rmPrefix,
                Boolean.valueOf(forceIfOnlyPrefix), resultPath);
    }

    private ILineProcess<Map<String, String>> getDeleteFile() throws IOException {
        String bucket = entryParam.getValue("bucket");
        return new DeleteFile(auth, configuration, bucket, resultPath);
    }

    private ILineProcess<Map<String, String>> getAsyncFetch() throws IOException {
        String toBucket = entryParam.getValue("to-bucket");
        String domain = entryParam.getValue("domain");
        String protocol = entryParam.getValue("protocol", null);
        if (!checkValue(protocol, "https?")) {
            throw new IOException("no incorrect protocol, please set it http or https");
        }
        String sign = entryParam.getValue("private", null);
        if (!checkValue(sign, "(true|false)")) {
            throw new IOException("no incorrect private, please set it true or false");
        }
        String keyPrefix = entryParam.getValue("add-prefix", null);
        String urlIndex = entryParam.getValue("url-index", null);
        String host = entryParam.getValue("host", null);
        String md5Index = entryParam.getValue("md5-index", null);
        String callbackUrl = entryParam.getValue("callback-url", null);
        String callbackBody = entryParam.getValue("callback-body", null);
        String callbackBodyType = entryParam.getValue("callback-body-type", null);
        String callbackHost = entryParam.getValue("callback-host", null);
        String type = entryParam.getValue("file-type", "0");
        String ignore = entryParam.getValue("ignore-same-key", "false");
        if (!checkValue(ignore, "(true|false)")) {
            throw new IOException("no incorrect ignore-same-key, please set it true or false");
        }
        ILineProcess<Map<String, String>> processor = new AsyncFetch(auth, configuration, toBucket, domain, protocol,
                Boolean.valueOf(sign), keyPrefix, urlIndex, resultPath);
        if (host != null || md5Index != null || callbackUrl != null || callbackBody != null || callbackBodyType != null
                || callbackHost != null || "1".equals(type) || "true".equals(ignore)) {
            ((AsyncFetch) processor).setFetchArgs(host, md5Index, callbackUrl, callbackBody,
                    callbackBodyType, callbackHost, Integer.valueOf(type), Boolean.valueOf(ignore));
        };
        return processor;
    }

    private ILineProcess<Map<String, String>> getQueryAvinfo() throws IOException {
        String domain = entryParam.getValue("domain");
        String protocol = entryParam.getValue("protocol", null);
        if (!checkValue(protocol, "https?")) {
            throw new IOException("no incorrect protocol, please set it http or https");
        }
        String urlIndex = entryParam.getValue("url-index");
        String sign = entryParam.getValue("private", null);
        if (!checkValue(sign, "(true|false)")) {
            throw new IOException("no incorrect private, please set it true or false");
        } else if (Boolean.valueOf(sign)) {
            String accessKey = entryParam.getValue("ak");
            String secretKey = entryParam.getValue("sk");
            auth = Auth.create(accessKey, secretKey);
        }
        return new QueryAvinfo(domain, protocol, urlIndex, auth, resultPath);
    }

    private ILineProcess<Map<String, String>> getQiniuPfop() throws IOException {
        String bucket = entryParam.getValue("bucket");
        String fopsIndex = entryParam.getValue("fops-index");
        String forcePublic = entryParam.getValue("force-public", "false");
        String pipeline = entryParam.getValue("pipeline", null);
        if (pipeline == null && !"true".equals(forcePublic)) {
            throw new IOException("please set pipeline, if you don't want to use" +
                    " private pipeline, please set the force-public as true.");
        }
        return new QiniuPfop(auth, configuration, bucket, pipeline, fopsIndex, resultPath);
    }

    private ILineProcess<Map<String, String>> getPfopResult() throws IOException {
        String persistentIdIndex = entryParam.getValue("persistentId-index");
        return new QueryPfopResult(persistentIdIndex, resultPath);
    }

    private ILineProcess<Map<String, String>> getQueryHash() throws IOException {
        String domain = entryParam.getValue("domain");
        String algorithm = entryParam.getValue("algorithm", null);
        if (!checkValue(algorithm, "(md5|sha1)")) {
            throw new IOException("no incorrect algorithm, please set it md5 or sha1");
        }
        String protocol = entryParam.getValue("protocol", null);
        if (!checkValue(protocol, "https?")) {
            throw new IOException("no incorrect protocol, please set it http or https");
        }
        String urlIndex = entryParam.getValue("url-index");
        String sign = entryParam.getValue("private", null);
        if (!checkValue(sign, "(true|false)")) {
            throw new IOException("no incorrect private, please set it true or false");
        } else if (Boolean.valueOf(sign)) {
            String accessKey = entryParam.getValue("ak");
            String secretKey = entryParam.getValue("sk");
            auth = Auth.create(accessKey, secretKey);
        }
        return new QueryHash(domain, algorithm, protocol, urlIndex, auth, resultPath);
    }

    private ILineProcess<Map<String, String>> getFileStat() throws IOException {
        String bucket = entryParam.getValue("bucket");
        return new FileStat(auth, configuration, bucket, resultPath, resultFormat);
    }

    private ILineProcess<Map<String, String>> getPrivateUrl() throws IOException {
        String domain = entryParam.getValue("domain");
        String protocol = entryParam.getValue("protocol", null);
        if (!checkValue(protocol, "https?")) {
            throw new IOException("no incorrect protocol, please set it http or https");
        }
        String urlIndex = entryParam.getValue("url-index");
        String expires = entryParam.getValue("expires", "3600");
        if (!checkValue(expires, "[1-9]\\d*")) {
            throw new IOException("no incorrect expires, please set it as a number.");
        }
        return new PrivateUrl(auth, domain, protocol, urlIndex, Long.valueOf(expires), resultPath);
    }
}
