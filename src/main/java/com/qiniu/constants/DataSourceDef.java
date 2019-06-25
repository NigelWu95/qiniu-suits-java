package com.qiniu.constants;

import java.util.HashSet;
import java.util.Set;

public class DataSourceDef {

    public static String S3 = "s3";
    public static String AWS = "aws";
    public static String QINIU = "qiniu";
    public static String TENCENT = "tencent";
    public static String ALIYUN = "aliyun";
    public static String UPYUN = "upyun";
    public static String NETYUN = "netease";
    public static String LOCAL = "local";

    public static Set<String> cloudStorage = new HashSet<String>(){{
        add(QINIU);
        add(TENCENT);
        add(ALIYUN);
        add(UPYUN);
        add(NETYUN);
        add(S3);
        add(AWS);
    }};

    /**
     * 文件列表数据源
     */
    public static Set<String> fileList = new HashSet<String>(){{
        add(LOCAL);
    }};
}
