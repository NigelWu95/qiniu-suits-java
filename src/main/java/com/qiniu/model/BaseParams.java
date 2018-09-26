package com.qiniu.model;

import com.qiniu.util.MainArgsUtils;

import java.util.Map;

public class BaseParams {

    private String accessKey;

    private String secretKey;

    private String selfName;

    protected BaseParams(String[] args) throws Exception {
        MainArgsUtils.setParamsMap(args);
        this.accessKey = MainArgsUtils.getParamValue("ak");
        this.secretKey = MainArgsUtils.getParamValue("sk");
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    protected void setSelfName(String name) {
        this.selfName = name;
    }

    public String getSelfName() {
        return selfName;
    }

    public String getParam(String key) throws Exception {
        return MainArgsUtils.getParamValue(key);
    }
}