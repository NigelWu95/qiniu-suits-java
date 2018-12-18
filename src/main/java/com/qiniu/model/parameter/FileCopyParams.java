package com.qiniu.model.parameter;

public class FileCopyParams extends QossParams {

    private String targetBucket;
    private String keepKey;
    private String keyPrefix;

    public FileCopyParams(String[] args) throws Exception {
        super(args);
        this.targetBucket = getParamFromArgs("to-bucket");
        try { this.keepKey = getParamFromArgs("keep-key"); } catch (Exception e) { keepKey = ""; }
        try { this.keyPrefix = getParamFromArgs("add-prefix"); } catch (Exception e) { keyPrefix = ""; }
    }

    public FileCopyParams(String configFileName) throws Exception {
        super(configFileName);
        this.targetBucket = getParamFromConfig("to-bucket");
        try { this.keepKey = getParamFromConfig("keep-key"); } catch (Exception e) { keepKey = ""; }
        try { this.keyPrefix = getParamFromConfig("add-prefix"); } catch (Exception e) { keyPrefix = ""; }
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
