package com.qiniu.util;

import java.util.ArrayList;
import java.util.List;

public final class ProcessUtils {

    private static List<String> needUrlProcesses = new ArrayList<String>(){{
        add("asyncfetch");
        add("privateurl");
        add("qhash");
        add("avinfo");
        add("exportts");
    }};
    private static List<String> needNewKeyProcesses = new ArrayList<String>(){{
        add("copy");
        add("rename");
    }};
    private static List<String> needFopsProcesses = new ArrayList<String>(){{
        add("pfop");
    }};
    private static List<String> needPidProcesses = new ArrayList<String>(){{
        add("pfopresult");
    }};
    private static List<String> needAvinfoProcesses = new ArrayList<String>(){{
        add("pfopcmd");
    }};
    private static List<String> needBucketAnKeyProcesses = new ArrayList<String>(){{
        add("status");
        add("type");
        add("lifecycle");
        add("mirror");
        add("delete");
        add("copy");
        add("rename");
        add("move");
        add("pfop");
        add("stat");
    }};
    private static List<String> needAuthProcesses = new ArrayList<String>(){{
        addAll(needBucketAnKeyProcesses);
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
    private static List<String> supportListSourceProcesses = new ArrayList<String>(){{
        addAll(needAuthProcesses);
        add("qhash");
        add("avinfo");
        add("exportts");
        add("filter");
    }};
    private static List<String> needConfigurationProcesses = new ArrayList<String>(){{
        addAll(needBucketAnKeyProcesses);
        addAll(needPidProcesses);
        add("asyncfetch");
        add("qhash");
        add("avinfo");
        add("exportts");
    }};

    public static boolean needUrl(String process) {
        return needUrlProcesses.contains(process);
    }

    public static boolean needNewKey(String process) {
        return needNewKeyProcesses.contains(process);
    }

    public static boolean needFops(String process) {
        return needFopsProcesses.contains(process);
    }

    public static boolean needPid(String process) {
        return needPidProcesses.contains(process);
    }

    public static boolean needAvinfo(String process) {
        return needAvinfoProcesses.contains(process);
    }

    public static boolean needBucketAndKey(String process) {
        return needBucketAnKeyProcesses.contains(process);
    }

    public static boolean needAuth(String process) {
        return needAuthProcesses.contains(process);
    }

    public static boolean canBatch(String process) {
        return canBatchProcesses.contains(process);
    }

    public static boolean supportListSource(String process) {
        return supportListSourceProcesses.contains(process);
    }

    public static boolean needConfiguration(String process) {
        return needConfigurationProcesses.contains(process);
    }
}
