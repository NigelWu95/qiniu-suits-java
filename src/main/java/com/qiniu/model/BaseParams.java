package com.qiniu.model;

import com.qiniu.common.QiniuException;
import com.qiniu.config.PropertyConfig;
import com.qiniu.util.MainArgsUtils;
import com.qiniu.util.StringUtils;

public class BaseParams {

    private String accessKey;
    private String secretKey;
    private String bucket;
    private PropertyConfig propertyConfig;

    protected BaseParams(String[] args) throws Exception {
        MainArgsUtils.setParamsMap(args);
        this.accessKey = MainArgsUtils.getParamValue("ak");
        this.secretKey = MainArgsUtils.getParamValue("sk");
        this.bucket = MainArgsUtils.getParamValue("bucket");
    }

    protected BaseParams(String configFileName) throws Exception {
        propertyConfig = new PropertyConfig(configFileName);
        this.accessKey = propertyConfig.getProperty("ak");
        this.secretKey = propertyConfig.getProperty("sk");
        this.bucket = propertyConfig.getProperty("bucket");
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

    public String getParamFromArgs(String key) throws Exception {
        return MainArgsUtils.getParamValue(key);
    }

    public String getParamFromConfig(String key) throws Exception {
        return propertyConfig.getProperty(key);
    }
}