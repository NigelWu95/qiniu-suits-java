package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

public class FileCopyParams extends QossParams {

    private String toBucket;
    private String keyPrefix;
    private String rmPrefix;

    public FileCopyParams(IEntryParam entryParam) throws Exception {
        super(entryParam);
        toBucket = entryParam.getParamValue("to-bucket");
        try { keyPrefix = entryParam.getParamValue("add-prefix"); } catch (Exception e) { keyPrefix = ""; }
        try { rmPrefix = entryParam.getParamValue("rm-prefix"); } catch (Exception e) { rmPrefix = ""; }
    }

    public String getToBucket() {
        return toBucket;
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public String getRmPrefix() {
        return rmPrefix;
    }
}
