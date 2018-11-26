package com.qiniu.model.parameter;

public class AsyncFetchParams extends QossParams {

    private String processAk = "";
    private String processSk = "";
    private String targetBucket;
    private String domain;
    private String https = "";
    private String needSign = "";
    private String keepKey = "";
    private String keyPrefix = "";
    private String hashCheck = "";
    private String host;
    private String callbackUrl;
    private String callbackBody;
    private String callbackBodyType;
    private String callbackHost;
    private String fileType = "";
    private String ignoreSameKey = "";

    public AsyncFetchParams(String[] args) throws Exception {
        super(args);
        try { this.processAk = getParamFromArgs("process-ak"); } catch (Exception e) {}
        try { this.processSk = getParamFromArgs("process-sk"); } catch (Exception e) {}
        this.targetBucket = getParamFromArgs("to-bucket");
        this.domain = getParamFromArgs("domain");
        try { this.https = getParamFromArgs("use-https"); } catch (Exception e) {}
        try { this.needSign = getParamFromArgs("need-sign"); } catch (Exception e) {}
        try { this.keepKey = getParamFromArgs("keep-key"); } catch (Exception e) {}
        try { this.keyPrefix = getParamFromArgs("add-prefix"); } catch (Exception e) {}
        try{ this.hashCheck = getParamFromArgs("hash-check"); } catch (Exception e) {}
        try{ this.host = getParamFromArgs("host"); } catch (Exception e) {}
        try{ this.callbackUrl = getParamFromArgs("callback-url"); } catch (Exception e) {}
        try{ this.callbackBody = getParamFromArgs("callback-body"); } catch (Exception e) {}
        try{ this.callbackBodyType = getParamFromArgs("callback-body-type"); } catch (Exception e) {}
        try{ this.callbackHost = getParamFromArgs("callback-host"); } catch (Exception e) {}
        try{ this.fileType = getParamFromArgs("file-type"); } catch (Exception e) {}
        try{ this.ignoreSameKey = getParamFromArgs("ignore-same-key"); } catch (Exception e) {}
    }

    public AsyncFetchParams(String configFileName) throws Exception {
        super(configFileName);
        try { this.processAk = getParamFromConfig("process-ak"); } catch (Exception e) {}
        try { this.processSk = getParamFromConfig("process-sk"); } catch (Exception e) {}
        this.targetBucket = getParamFromConfig("to-bucket");
        this.domain = getParamFromConfig("domain");
        try { this.https = getParamFromConfig("use-https"); } catch (Exception e) {}
        try { this.needSign = getParamFromConfig("need-sign"); } catch (Exception e) {}
        try { this.keepKey = getParamFromConfig("keep-key"); } catch (Exception e) {}
        try { this.keyPrefix = getParamFromConfig("add-prefix"); } catch (Exception e) {}
        try{ this.hashCheck = getParamFromConfig("hash-check"); } catch (Exception e) {}
        try{ this.host = getParamFromConfig("host"); } catch (Exception e) {}
        try{ this.callbackUrl = getParamFromConfig("callback-url"); } catch (Exception e) {}
        try{ this.callbackBody = getParamFromConfig("callback-body"); } catch (Exception e) {}
        try{ this.callbackBodyType = getParamFromConfig("callback-body-type"); } catch (Exception e) {}
        try{ this.callbackHost = getParamFromConfig("callback-host"); } catch (Exception e) {}
        try{ this.fileType = getParamFromConfig("file-type"); } catch (Exception e) {}
        try{ this.ignoreSameKey = getParamFromConfig("ignore-same-key"); } catch (Exception e) {}
    }

    public String getProcessAk() {
        return processAk;
    }

    public String getProcessSk() {
        return processSk;
    }

    public String getTargetBucket() {
        return targetBucket;
    }

    public String getDomain() {
        return domain;
    }

    public boolean getHttps() {
        if (https.matches("(true|false)")) {
            return Boolean.valueOf(https);
        } else {
            System.out.println("no incorrect use-https, it will use false as default.");
            return false;
        }
    }

    public boolean getNeedSign() {
        if (needSign.matches("(true|false)")) {
            return Boolean.valueOf(needSign);
        } else {
            System.out.println("no incorrect need-sign, it will use false as default.");
            return false;
        }
    }

    public boolean getKeepKey() {
        if (keepKey.matches("(true|false)")) {
            return Boolean.valueOf(keepKey);
        } else {
            System.out.println("no incorrect keep-key, it will use true as default.");
            return true;
        }
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public boolean getHashCheck() {
        if (hashCheck.matches("(true|false)")) {
            return Boolean.valueOf(hashCheck);
        } else {
            System.out.println("no incorrect hash-check, it will use false as default.");
            return false;
        }
    }

    public String getHost() {
        return host;
    }

    public String getCallbackUrl() {
        return callbackUrl;
    }

    public String getCallbackBody() {
        return callbackBody;
    }

    public String getCallbackBodyType() {
        return callbackBodyType;
    }

    public String getCallbackHost() {
        return callbackHost;
    }

    public int getFileType() {
        if (fileType.matches("(0|1)")) {
            return Short.valueOf(fileType);
        } else {
            System.out.println("no incorrect file-type, please set it 0 or 1");
            return 0;
        }
    }

    public boolean getIgnoreSameKey() {
        if (ignoreSameKey.matches("(true|false)")) {
            return Boolean.valueOf(ignoreSameKey);
        } else {
            System.out.println("no incorrect ignore-same-key, it will use false as default.");
            return false;
        }
    }

    public boolean hasCustomArgs() {
        return (host != null || callbackUrl != null || callbackBody != null || callbackBodyType != null
                || callbackHost != null || "1".equals(fileType) || "true".equals(ignoreSameKey));
    }
}
