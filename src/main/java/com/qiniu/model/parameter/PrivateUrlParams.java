package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;

public class PrivateUrlParams extends QossParams {

    private String domain;
    private String https;
    private String expires;

    public PrivateUrlParams(IEntryParam entryParam) throws Exception {
        super(entryParam);
        this.domain = entryParam.getParamValue("domain");
        try { https = entryParam.getParamValue("https"); } catch (Exception e) { https = ""; }
        try { expires = entryParam.getParamValue("expires"); } catch (Exception e) { expires = ""; }
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

    public Long getExpires() throws IOException {
        if ("".equals(expires)) {
            return 3600L;
        } else if (expires.matches("[1-9]\\d*")) {
            return Long.valueOf(expires);
        } else {
            throw new IOException("please set expires as a long number.");
        }
    }
}
