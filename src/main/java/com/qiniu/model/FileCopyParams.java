package com.qiniu.model;

public class FileCopyParams extends BaseParams {

    private String aKey = "";
    private String sKey = "";
    private String sourceBucket;
    private String targetBucket;
    private String keepKey;
    private String targetKeyPrefix = "";
    private PointTimeParams pointTimeParams;

    public FileCopyParams(String[] args) throws Exception {
        super(args);
        this.pointTimeParams = new PointTimeParams(args);
        try {
            this.aKey = getParamFromArgs("access-key");
            this.sKey = getParamFromArgs("secret-key");
        } catch (Exception e) {}
        this.sourceBucket = getParamFromArgs("from");
        this.targetBucket = getParamFromArgs("to");
        this.keepKey = getParamFromArgs("keep-key");
        try {
            this.targetKeyPrefix = getParamFromArgs("add-prefix");
        } catch (Exception e) {}
    }

    public FileCopyParams(String configFileName) throws Exception {
        super(configFileName);
        pointTimeParams = new PointTimeParams(configFileName);
        try {
            this.aKey = getParamFromConfig("access-key");
            this.sKey = getParamFromConfig("secret-key");
        } catch (Exception e) {}
        this.sourceBucket = getParamFromConfig("from");
        this.targetBucket = getParamFromConfig("to");
        this.keepKey = getParamFromConfig("keep-key");
        try {
            this.targetKeyPrefix = getParamFromConfig("add-prefix");
        } catch (Exception e) {}
    }

    public String getAKey() {
        return aKey;
    }

    public String getSKey() {
        return sKey;
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
            System.out.println("the keep-key is incorrect, it will use true as default.");
            return true;
        }
    }

    public String getTargetKeyPrefix() {
        return targetKeyPrefix;
    }

    public PointTimeParams getPointTimeParams() {
        return pointTimeParams;
    }
}