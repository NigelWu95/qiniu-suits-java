package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class QossParams extends CommonParams {

    private String accessKey;
    private String secretKey;
    private String bucket;
    private List<String> needAkSkProcesses = new ArrayList<String>(){{
        add("status");
        add("type");
        add("lifecycle");
        add("copy");
        add("move");
        add("rename");
        add("delete");
        add("asyncfetch");
        add("pfop");
        add("stat");
        add("privateurl");
    }};

    public QossParams(IEntryParam entryParam) {
        super(entryParam);
        try { accessKey = entryParam.getParamValue("ak"); } catch (Exception e) {}
        try { secretKey = entryParam.getParamValue("sk"); } catch (Exception e) {}
        try { bucket = entryParam.getParamValue("bucket"); } catch (Exception e) {}
    }

    public String getAccessKey() throws IOException {
        if (accessKey == null || "".equals(accessKey)) {
            if ("list".equals(getSourceType()) || needAkSkProcesses.contains(getProcess()))
                throw new IOException("no incorrect ak, please set it.");
            else return "";
        } else {
            return accessKey;
        }
    }

    public String getSecretKey() throws IOException {
        if (secretKey == null || "".equals(secretKey)) {
            if ("list".equals(getSourceType()) || needAkSkProcesses.contains(getProcess()))
                throw new IOException("no incorrect sk, please set it.");
            else return "";
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
}
