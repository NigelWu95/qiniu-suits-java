package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;

public class QhashParams extends QossParams {

    private String domain;
    private String algorithm;
    private String https;
    private String needSign;
    private String urlIndex;

    public QhashParams(IEntryParam entryParam) throws Exception {
        super(entryParam);
        this.domain = entryParam.getParamValue("domain");
        try { this.algorithm = entryParam.getParamValue("algorithm"); } catch (Exception e) { algorithm = ""; }
        try { this.https = entryParam.getParamValue("https"); } catch (Exception e) { https = ""; }
        try { this.needSign = entryParam.getParamValue("private"); } catch (Exception e) { needSign = ""; }
        try { this.urlIndex = entryParam.getParamValue("url-index"); } catch (Exception e) { urlIndex = ""; }
    }

    public String getDomain() {
        return domain;
    }

    public String getAlgorithm() {
        if (algorithm.matches("(md5|sha1)")) {
            return algorithm;
        } else {
            return "md5";
        }
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
}
