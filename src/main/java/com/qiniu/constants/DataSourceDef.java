package com.qiniu.constants;

import java.util.HashSet;
import java.util.Set;

public class DataSourceDef {

    public static String QINIU = "qiniu";
    public static String TENCENT = "tencent";
    public static String ALIYUN = "aliyun";
    public static String LOCAL = "local";

    public static Set<String> ossListSource = new HashSet<String>(){{
        add(QINIU);
        add(TENCENT);
        add(ALIYUN);
    }};

    public static Set<String> fileSource = new HashSet<String>(){{
        add(LOCAL);
    }};
}
