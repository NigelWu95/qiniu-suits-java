package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

public class FileCopyParams extends QossParams {

    private String targetBucket;
    private String keepKey;
    private String keyPrefix;
    private String rmPrefix;

    public FileCopyParams(IEntryParam entryParam) throws Exception {
        super(entryParam);
        this.targetBucket = entryParam.getParamValue("to-bucket");
        try { this.keepKey = entryParam.getParamValue("keep-key"); } catch (Exception e) { keepKey = ""; }
        try { this.keyPrefix = entryParam.getParamValue("add-prefix"); } catch (Exception e) { keyPrefix = ""; }
        try { this.rmPrefix = entryParam.getParamValue("rm-prefix"); } catch (Exception e) { rmPrefix = ""; }
    }

    public String getTargetBucket() {
        return targetBucket;
    }

    public boolean getKeepKey() {
        if (keepKey.matches("(true|false)")) {
            return Boolean.valueOf(keepKey);
        } else {
            return true;
        }
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public String getRmPrefix() {
        return rmPrefix;
    }
}
