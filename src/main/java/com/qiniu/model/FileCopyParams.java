package com.qiniu.model;

public class FileCopyParams extends BaseParams {

    private String processAk = "";
    private String processSk = "";
    private String targetBucket;
    private String keepKey = "";
    private String keyPrefix = "";

    public FileCopyParams(String[] args) throws Exception {
        super(args);
        try { this.processAk = getParamFromArgs("process-ak"); } catch (Exception e) {}
        try { this.processSk = getParamFromArgs("process-sk"); } catch (Exception e) {}
        this.targetBucket = getParamFromArgs("to-bucket");
        try { this.keepKey = getParamFromArgs("keep-key"); } catch (Exception e) {}
        try { this.keyPrefix = getParamFromArgs("add-prefix"); } catch (Exception e) {}
    }

    public FileCopyParams(String configFileName) throws Exception {
        super(configFileName);
        try { this.processAk = getParamFromConfig("process-ak"); } catch (Exception e) {}
        try { this.processSk = getParamFromConfig("process-sk"); } catch (Exception e) {}
        this.targetBucket = getParamFromConfig("to-bucket");
        try { this.keepKey = getParamFromConfig("keep-key"); } catch (Exception e) {}
        try { this.keyPrefix = getParamFromConfig("add-prefix"); } catch (Exception e) {}
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
}
