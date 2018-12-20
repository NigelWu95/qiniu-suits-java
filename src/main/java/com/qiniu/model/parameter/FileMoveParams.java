package com.qiniu.model.parameter;

import java.io.IOException;

public class FileMoveParams extends QossParams {

    private String targetBucket;
    private String keyPrefix;

    public FileMoveParams(String[] args) throws Exception {
        super(args);
        try { this.targetBucket = getParamFromArgs("to-bucket"); } catch (Exception e) {}
        try { this.keyPrefix = getParamFromArgs("add-prefix"); } catch (Exception e) { keyPrefix = ""; }
    }

    public FileMoveParams(String configFileName) throws Exception {
        super(configFileName);
        try { this.targetBucket = getParamFromConfig("to-bucket"); } catch (Exception e) {}
        try { this.keyPrefix = getParamFromConfig("add-prefix"); } catch (Exception e) { keyPrefix = ""; }
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
