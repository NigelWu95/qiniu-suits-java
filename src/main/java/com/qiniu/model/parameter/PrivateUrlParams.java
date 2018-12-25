package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;

public class PrivateUrlParams extends QossParams {

    private String domain;
    private String https;
    private String urlIndex;
    private String expires;

    public PrivateUrlParams(IEntryParam entryParam) throws Exception {
        super(entryParam);
        this.domain = entryParam.getParamValue("domain");
        try { this.https = entryParam.getParamValue("https"); } catch (Exception e) { https = ""; }
        try { this.urlIndex = entryParam.getParamValue("url-index"); } catch (Exception e) { urlIndex = ""; }
        try { this.expires = entryParam.getParamValue("expires"); } catch (Exception e) { expires = ""; }
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

    public String getUrlIndex() throws IOException {
        if ("json".equals(getParseType())) {
            if ("".equals(urlIndex)) {
                throw new IOException("no incorrect json key index for avinfo's url.");
            } else {
                return urlIndex;
            }
        } else if ("table".equals(getParseType())) {
            if ("".equals(urlIndex)) {
                return "0";
            } else if (urlIndex.matches("\\d")) {
                return urlIndex;
            } else {
                throw new IOException("no incorrect url index, it should be a number.");
            }
        } else {
            throw new IOException("no incorrect object key index for avinfo's url.");
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
