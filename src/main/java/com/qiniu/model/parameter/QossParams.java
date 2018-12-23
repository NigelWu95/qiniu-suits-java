package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;

public class QossParams extends CommonParams {

    private String accessKey;
    private String secretKey;
    private String bucket;
    private String processAk;
    private String processSk;

    public QossParams(IEntryParam entryParam) throws IOException {
        super(entryParam);
        try { this.accessKey = entryParam.getParamValue("ak"); } catch (Exception e) {}
        try { this.secretKey = entryParam.getParamValue("sk"); } catch (Exception e) {}
        try { this.bucket = entryParam.getParamValue("bucket"); } catch (Exception e) {}
        try { this.processAk = entryParam.getParamValue("process-ak"); } catch (Exception e) {}
        try { this.processSk = entryParam.getParamValue("process-sk"); } catch (Exception e) {}
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
            if (secretKey == null || "".equals(secretKey))
                throw new IOException("no incorrect sk, please set it.");
            else return secretKey;
        } else {
            return secretKey;
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
