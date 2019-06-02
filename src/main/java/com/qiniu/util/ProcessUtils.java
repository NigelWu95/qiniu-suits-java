package com.qiniu.util;

import java.util.HashSet;
import java.util.Set;

public final class ProcessUtils {

    private static Set<String> needUrlProcesses = new HashSet<String>(){{
        add("asyncfetch");
        add("privateurl");
        add("qhash");
        add("avinfo");
        add("exportts");
    }};
    private static Set<String> needNewKeyProcesses = new HashSet<String>(){{
        add("copy");
        add("rename");
    }};
    private static Set<String> needFopsProcesses = new HashSet<String>(){{
        add("pfop");
    }};
    private static Set<String> needPidProcesses = new HashSet<String>(){{
        add("pfopresult");
    }};
    private static Set<String> needAvinfoProcesses = new HashSet<String>(){{
        add("pfopcmd");
    }};
    private static Set<String> needBucketAnKeyProcesses = new HashSet<String>(){{
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
    private static Set<String> needAuthProcesses = new HashSet<String>(){{
        addAll(needBucketAnKeyProcesses);
        add("asyncfetch");
        add("privateurl");
    }};
    private static Set<String> canBatchProcesses = new HashSet<String>(){{
        add("status");
        add("type");
        add("lifecycle");
        add("copy");
        add("move");
        add("rename");
        add("delete");
        add("stat");
    }};
    private static Set<String> supportListSourceProcesses = new HashSet<String>(){{
        addAll(needAuthProcesses);
        add("qhash");
        add("avinfo");
        add("exportts");
        add("filter");
    }};
    private static Set<String> needConfigurationProcesses = new HashSet<String>(){{
        addAll(needBucketAnKeyProcesses);
        addAll(needPidProcesses);
        add("asyncfetch");
        add("qhash");
        add("avinfo");
        add("exportts");
    }};

    private static Set<String> processes = new HashSet<String>(){{
        addAll(needUrlProcesses);
        addAll(needNewKeyProcesses);
        addAll(needFopsProcesses);
        addAll(needPidProcesses);
        addAll(needAvinfoProcesses);
        addAll(needBucketAnKeyProcesses);
        addAll(supportListSourceProcesses);
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

    public static boolean isSupportedProcess(String process) {
        return processes.contains(process);
    }
}
