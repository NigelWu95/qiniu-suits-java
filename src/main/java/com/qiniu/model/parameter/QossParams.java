package com.qiniu.model.parameter;

import com.qiniu.common.QiniuException;
import com.qiniu.util.StringUtils;

import java.io.IOException;

public class QossParams extends CommonParams {

    private String accessKey;
    private String secretKey;
    private String bucket;
    private String processAk;
    private String processSk;

    public QossParams(String[] args) throws IOException {
        super(args);
        this.accessKey = getParamFromArgs("ak");
        this.secretKey = getParamFromArgs("sk");
        this.bucket = getParamFromArgs("bucket");
        try { this.processAk = getParamFromArgs("process-ak"); } catch (Exception e) {}
        try { this.processSk = getParamFromArgs("process-sk"); } catch (Exception e) {}
    }

    public QossParams(String configFileName) throws IOException {
        super(configFileName);
        this.accessKey = getParamFromConfig("ak");
        this.secretKey = getParamFromConfig("sk");
        this.bucket = getParamFromConfig("bucket");
        try { this.processAk = getParamFromConfig("process-ak"); } catch (Exception e) {}
        try { this.processSk = getParamFromConfig("process-sk"); } catch (Exception e) {}
    }

    public String getAccessKey() throws QiniuException {
        if (StringUtils.isNullOrEmpty(accessKey)) {
            throw new QiniuException(null, "no incorrect ak, please set it.");
        } else {
            return accessKey;
        }
    }

    public String getSecretKey() throws QiniuException {
        if (StringUtils.isNullOrEmpty(secretKey)) {
            throw new QiniuException(null, "no incorrect sk, please set it.");
        } else {
            return secretKey;
        }
    }

    public String getBucket() throws QiniuException {
        if (StringUtils.isNullOrEmpty(bucket)) {
            throw new QiniuException(null, "no incorrect bucket, please set it.");
        } else {
            return bucket;
        }
    }

    public String getProcessAk() {
        if (processAk == null || "".equals(processAk)) processAk = accessKey;
        return processAk;
    }

    public String getProcessSk() {
        if (processSk == null || "".equals(processSk)) processSk = secretKey;
        return processSk;
    }
}
