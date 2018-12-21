package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;

public class FileMoveParams extends QossParams {

    private String targetBucket;
    private String keyPrefix;

    public FileMoveParams(IEntryParam entryParam) throws Exception {
        super(entryParam);
        try { this.targetBucket = entryParam.getParamValue("to-bucket"); } catch (Exception e) {}
        try { this.keyPrefix = entryParam.getParamValue("add-prefix"); } catch (Exception e) { keyPrefix = ""; }
    }

    public String getTargetBucket() throws IOException {
        if ("move".equals(getProcess())) {
            if (this.targetBucket == null || "".equals(this.targetBucket))
                throw new IOException("no incorrect to-bucket, please set it.");
            else return targetBucket;
        }
        return targetBucket;
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }
}
