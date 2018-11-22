package com.qiniu.model;

public class AvinfoParams extends BaseParams {

    private String processAk = "";
    private String processSk = "";
    private String targetBucket;
    private String domain;
    private String https = "";
    private String needSign = "";

    public AvinfoParams(String[] args) throws Exception {
        super(args);
        try { this.processAk = getParamFromArgs("process-ak"); } catch (Exception e) {}
        try { this.processSk = getParamFromArgs("process-sk"); } catch (Exception e) {}
        this.targetBucket = getParamFromArgs("to-bucket");
        this.domain = getParamFromArgs("domain");
        try { this.https = getParamFromArgs("use-https"); } catch (Exception e) {}
        try { this.needSign = getParamFromArgs("need-sign"); } catch (Exception e) {}
    }

    public AvinfoParams(String configFileName) throws Exception {
        super(configFileName);
        try { this.processAk = getParamFromConfig("process-ak"); } catch (Exception e) {}
        try { this.processSk = getParamFromConfig("process-sk"); } catch (Exception e) {}
        this.targetBucket = getParamFromConfig("to-bucket");
        this.domain = getParamFromConfig("domain");
        try { this.https = getParamFromConfig("use-https"); } catch (Exception e) {}
        try { this.needSign = getParamFromConfig("need-sign"); } catch (Exception e) {}
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
}
