package com.qiniu.model;

public class FileCopyParams extends BaseParams {

    private String processAk = "";
    private String processSk = "";
    private String sourceBucket;
    private String targetBucket;
    private String keepKey;
    private String targetKeyPrefix = "";

    public FileCopyParams(String[] args) throws Exception {
        super(args);
        try { this.processAk = getParamFromArgs("process-ak"); } catch (Exception e) {}
        try { this.processSk = getParamFromArgs("process-sk"); } catch (Exception e) {}
        this.sourceBucket = getParamFromArgs("bucket1");
        this.targetBucket = getParamFromArgs("bucket2");
        try { this.keepKey = getParamFromArgs("keep-key"); } catch (Exception e) {}
        try { this.targetKeyPrefix = getParamFromArgs("add-prefix"); } catch (Exception e) {}
    }

    public FileCopyParams(String configFileName) throws Exception {
        super(configFileName);
        try { this.processAk = getParamFromConfig("process-ak"); } catch (Exception e) {}
        try { this.processSk = getParamFromConfig("process-sk"); } catch (Exception e) {}
        this.sourceBucket = getParamFromConfig("bucket1");
        this.targetBucket = getParamFromConfig("bucket2");
        try { this.keepKey = getParamFromConfig("keep-key"); } catch (Exception e) {}
        try { this.targetKeyPrefix = getParamFromConfig("add-prefix"); } catch (Exception e) {}
    }

    public String getProcessAk() {
        return processAk;
    }

    public String getProcessSk() {
        return processSk;
    }

    public String getSourceBucket() {
        return sourceBucket;
    }

    public String getTargetBucket() {
        return targetBucket;
    }

    public boolean getKeepKey() {
        if (keepKey.matches("(true|false)")) {
            return Boolean.valueOf(keepKey);
        } else {
            System.out.println("no incorrect keep-key, it will use true as default.");
            return true;
        }
    }

    public String getTargetKeyPrefix() {
        return targetKeyPrefix;
    }
}
