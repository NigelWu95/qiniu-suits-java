package com.qiniu.constants;

import java.util.ArrayList;
import java.util.List;

public class DataSourceDef {

    public static String QINIU = "qiniu";
    public static String TENCENT = "tencent";
    public static String ALIYUN = "aliyun";
    public static String LOCAL = "local";

    public static List<String> ossListSource = new ArrayList<String>(){{
        add(QINIU);
        add(TENCENT);
        add(ALIYUN);
    }};

    public static List<String> fileSource = new ArrayList<String>(){{
        add(LOCAL);
    }};
}
