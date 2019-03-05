package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;

public class AvinfoParams extends QossParams {

    private String domain;
    private String https;
    private String needSign;

    public AvinfoParams(IEntryParam entryParam) throws Exception {
        super(entryParam);
        domain = entryParam.getParamValue("domain");
        try { https = entryParam.getParamValue("https"); } catch (Exception e) { https = ""; }
        try { needSign = entryParam.getParamValue("private"); } catch (Exception e) { needSign = ""; }
    }

    public String getDomain() {
        return domain;
    }

    public String getProtocol() throws IOException {
        if ("".equals(https) || https.matches("false")) {
            return "http";
        } else if (https.matches("true")) {
            return "https";
        } else {
            throw new IOException("please set https as true/false.");
        }
    }

    public boolean getNeedSign() {
        if (needSign.matches("(true|false)")) {
            return Boolean.valueOf(needSign);
        } else {
            return false;
        }
    }
}
