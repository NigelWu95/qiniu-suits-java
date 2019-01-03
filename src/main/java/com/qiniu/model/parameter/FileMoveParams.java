package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;

public class FileMoveParams extends QossParams {

    private String toBucket;
    private String keyPrefix;
    private String forceIfOnlyPrefix;
    private String rmPrefix;

    public FileMoveParams(IEntryParam entryParam) throws Exception {
        super(entryParam);
        try { this.toBucket = entryParam.getParamValue("to-bucket"); } catch (Exception e) {}
        try { this.keyPrefix = entryParam.getParamValue("add-prefix"); } catch (Exception e) { keyPrefix = ""; }
        try { this.forceIfOnlyPrefix = entryParam.getParamValue("prefix-force"); } catch (Exception e) { forceIfOnlyPrefix = ""; }
        try { this.rmPrefix = entryParam.getParamValue("rm-prefix"); } catch (Exception e) { rmPrefix = ""; }
    }

    public String getToBucket() throws IOException {
        if ("move".equals(getProcess())) {
            if (toBucket == null || "".equals(toBucket))
                throw new IOException("no incorrect to-bucket, please set it.");
            else return toBucket;
        }
        return toBucket;
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public Boolean getForceIfOnlyPrefix() throws IOException {
        if ("".equals(forceIfOnlyPrefix) || forceIfOnlyPrefix.equals("false")) return false;
        else if (forceIfOnlyPrefix.equals("true")) {
            return true;
        } else {
            throw new IOException("prefix-force should be true/force.");
        }
    }

    public String getRmPrefix() {
        return rmPrefix;
    }
}
