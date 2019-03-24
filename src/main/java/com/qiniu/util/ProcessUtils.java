package com.qiniu.util;

import java.util.ArrayList;
import java.util.List;

public class ProcessUtils {

    private static List<String> needUrl = new ArrayList<String>(){{
        add("asyncfetch");
        add("privateurl");
        add("qhash");
        add("avinfo");
    }};
    private static List<String> needMd5 = new ArrayList<String>(){{
        add("asyncfetch");
    }};
    private static List<String> needNewKey = new ArrayList<String>(){{
        add("rename");
        add("copy");
    }};
    private static List<String> needFops = new ArrayList<String>(){{
        add("pfop");
    }};
    private static List<String> needPid = new ArrayList<String>(){{
        add("pfopresult");
    }};
    private static List<String> needAvinfo = new ArrayList<String>(){{
        add("pfopcmd");
    }};
    private static List<String> needBucketProcesses = new ArrayList<String>(){{
        add("status");
        add("type");
        add("lifecycle");
        add("copy");
        add("move");
        add("rename");
        add("delete");
        add("pfop");
        add("stat");
        add("mirror");
    }};
    private static List<String> needAuthProcesses = new ArrayList<String>(){{
        addAll(needBucketProcesses);
        add("asyncfetch");
        add("privateurl");
    }};
    private static List<String> canBatchProcesses = new ArrayList<String>(){{
        add("status");
        add("type");
        add("lifecycle");
        add("copy");
        add("move");
        add("rename");
        add("delete");
        add("stat");
    }};
}
