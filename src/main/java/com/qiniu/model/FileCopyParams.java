package com.qiniu.model;

import com.qiniu.util.StringUtils;

public class FileCopyParams extends BaseParams {

    private String aKey = "";
    private String sKey = "";
    private String sourceBucket;
    private String targetBucket;
    private String keepKey;
    private String targetKeyPrefix = "";

    public FileCopyParams(String[] args) throws Exception {
        super(args);
        this.sourceBucket = getParamFromArgs("from");
        this.targetBucket = getParamFromArgs("to");
        try { this.aKey = getParamFromArgs("access-key"); } catch (Exception e) {}
        try { this.sKey = getParamFromArgs("secret-key"); } catch (Exception e) {}
        try { this.keepKey = getParamFromArgs("keep-key"); } catch (Exception e) {}
        try { this.targetKeyPrefix = getParamFromArgs("add-prefix"); } catch (Exception e) {}
    }

    public FileCopyParams(String configFileName) throws Exception {
        super(configFileName);
        this.sourceBucket = getParamFromConfig("from");
        this.targetBucket = getParamFromConfig("to");
        try { this.aKey = getParamFromConfig("access-key"); } catch (Exception e) {}
        try { this.sKey = getParamFromConfig("secret-key"); } catch (Exception e) {}
        try { this.keepKey = getParamFromConfig("keep-key"); } catch (Exception e) {}
        try { this.targetKeyPrefix = getParamFromConfig("add-prefix"); } catch (Exception e) {}
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
        if (StringUtils.isNullOrEmpty(keepKey) || !keepKey.matches("(true|false)")) {
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