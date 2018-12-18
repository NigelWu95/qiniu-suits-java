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
        try { this.accessKey = getParamFromArgs("ak"); } catch (Exception e) {}
        try { this.secretKey = getParamFromArgs("sk"); } catch (Exception e) {}
        try { this.bucket = getParamFromArgs("bucket"); } catch (Exception e) {}
        try { this.processAk = getParamFromArgs("process-ak"); } catch (Exception e) {}
        try { this.processSk = getParamFromArgs("process-sk"); } catch (Exception e) {}
    }

    public QossParams(String configFileName) throws IOException {
        super(configFileName);
        try { this.accessKey = getParamFromConfig("ak"); } catch (Exception e) {}
        try { this.secretKey = getParamFromConfig("sk"); } catch (Exception e) {}
        try { this.bucket = getParamFromConfig("bucket"); } catch (Exception e) {}
        try { this.processAk = getParamFromConfig("process-ak"); } catch (Exception e) {}
        try { this.processSk = getParamFromConfig("process-sk"); } catch (Exception e) {}
    }

    public String getAccessKey() throws IOException {
        if ("list".equals(getSourceType())) {
            if (accessKey == null || "".equals(accessKey))
                throw new IOException("no incorrect ak, please set it.");
            else return accessKey;
        } else {
            return accessKey;
        }
    }

    public String getSecretKey() throws IOException {
        if ("list".equals(getSourceType())) {
            if (accessKey == null || "".equals(accessKey))
                throw new IOException("no incorrect sk, please set it.");
            else return accessKey;
        } else {
            return accessKey;
        }
    }

    public String getBucket() throws IOException {
        if (bucket == null || "".equals(bucket)) {
            throw new IOException("no incorrect bucket, please set it.");
        } else {
            return bucket;
        }
    }

    public String getProcessAk() throws IOException {
        if (processAk == null || "".equals(processAk)) return getAccessKey();
        return processAk;
    }

    public String getProcessSk() throws IOException {
        if (processSk == null || "".equals(processSk)) return getSecretKey();
        return processSk;
    }
}
