package com.qiniu.model;

import com.qiniu.config.PropertyConfig;
import com.qiniu.util.MainArgsUtils;

public class BaseParams {

    private String accessKey;
    private String secretKey;
    private String bucket;
    private String resultFileDir;
    private PropertyConfig propertyConfig;

    protected BaseParams(String[] args) throws Exception {
        MainArgsUtils.setParamsMap(args);
        this.accessKey = MainArgsUtils.getParamValue("ak");
        this.secretKey = MainArgsUtils.getParamValue("sk");
        this.bucket = MainArgsUtils.getParamValue("bucket");
        this.resultFileDir = MainArgsUtils.getParamValue("result-path");
    }

    protected BaseParams(String configFileName) throws Exception {
        propertyConfig = new PropertyConfig(configFileName);
        this.accessKey = propertyConfig.getProperty("ak");
        this.secretKey = propertyConfig.getProperty("sk");
        this.bucket = propertyConfig.getProperty("bucket");
        this.resultFileDir = propertyConfig.getProperty("result-path");
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getBucket() {
        return bucket;
    }

    public String getResultFileDir() {
        return System.getProperty("user.dir") + System.getProperty("file.separator") + resultFileDir;
    }

    public String getParamFromArgs(String key) throws Exception {
        return MainArgsUtils.getParamValue(key);
    }

    public String getParamFromConfig(String key) throws Exception {
        return propertyConfig.getProperty(key);
    }
}