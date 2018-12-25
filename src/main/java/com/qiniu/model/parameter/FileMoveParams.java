package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;

public class FileMoveParams extends QossParams {

    private String toBucket;
    private String keyPrefix;
    private String forceIfOnlyPrefix;
    private String newKeyIndex;

    public FileMoveParams(IEntryParam entryParam) throws Exception {
        super(entryParam);
        try { this.toBucket = entryParam.getParamValue("to-bucket"); } catch (Exception e) {}
        try { this.keyPrefix = entryParam.getParamValue("add-prefix"); } catch (Exception e) { keyPrefix = ""; }
        try { this.forceIfOnlyPrefix = entryParam.getParamValue("prefix-force"); } catch (Exception e) { forceIfOnlyPrefix = ""; }
        try { this.newKeyIndex = entryParam.getParamValue("newKey-index"); } catch (Exception e) { newKeyIndex = ""; }
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

    public String getNewKeyIndex() throws IOException {
        if ("json".equals(getParseType())) {
            if ("".equals(newKeyIndex)) {
                throw new IOException("no incorrect json key index for rename's newKey.");
            } else {
                return newKeyIndex;
            }
        } else if ("table".equals(getParseType())) {
            if ("".equals(newKeyIndex)) {
                return "1";
            } else if (newKeyIndex.matches("\\d")) {
                return newKeyIndex;
            } else {
                throw new IOException("no incorrect newKey index, it should be a number.");
            }
        } else {
            return "key";
        }
    }
}
