package com.qiniu.util;

import java.util.HashSet;
import java.util.Set;

public final class ProcessUtils {

    public static Set<String> needUrlProcesses = new HashSet<String>(){{
        add("asyncfetch");
        add("privateurl");
        add("qhash");
        add("avinfo");
        add("exportts");
    }};
    public static Set<String> needToKeyProcesses = new HashSet<String>(){{
        add("copy");
        add("move");
        add("rename");
    }};
    public static Set<String> needFopsProcesses = new HashSet<String>(){{
        add("pfop");
    }};
    public static Set<String> needPidProcesses = new HashSet<String>(){{
        add("pfopresult");
    }};
    public static Set<String> needAvinfoProcesses = new HashSet<String>(){{
        add("pfopcmd");
    }};
    public static Set<String> qiniuProcessesWithBucket = new HashSet<String>(){{
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
    public static Set<String> canBatchProcesses = new HashSet<String>(){{
        add("status");
        add("type");
        add("lifecycle");
        add("copy");
        add("move");
        add("rename");
        add("delete");
        add("stat");
    }};
    public static Set<String> tenProcesses = new HashSet<String>(){{
        add("tenprivate");
    }};
    public static Set<String> awsProcesses = new HashSet<String>(){{
        add("awsprivate");
    }};
    public static Set<String> aliProcesses = new HashSet<String>(){{
        add("aliprivate");
    }};
    public static Set<String> needBucketProcesses = new HashSet<String>(){{
        addAll(qiniuProcessesWithBucket);
        addAll(tenProcesses);
        addAll(awsProcesses);
        addAll(aliProcesses);
    }};
    public static Set<String> needQiniuAuthProcesses = new HashSet<String>(){{
        addAll(qiniuProcessesWithBucket);
        add("asyncfetch");
        add("privateurl");
    }};
    public static Set<String> supportListSourceProcesses = new HashSet<String>(){{
        addAll(needQiniuAuthProcesses);
        add("qhash");
        add("avinfo");
        add("exportts");
        add("filter");
    }};
    public static Set<String> dangerousProcesses = new HashSet<String>(){{
        add("status");
        add("move");
        add("rename");
        add("delete");
    }};
//    public static Set<String> processes = new HashSet<String>(){{
//        addAll(needUrlProcesses);
//        addAll(needToKeyProcesses);
//        addAll(needFopsProcesses);
//        addAll(needPidProcesses);
//        addAll(needAvinfoProcesses);
//        addAll(needBucketAnKeyProcesses);
//        addAll(supportListSourceProcesses);
//    }};

    public static boolean needUrl(String process) {
        return needUrlProcesses.contains(process);
    }

    public static boolean needToKey(String process) {
        return needToKeyProcesses.contains(process);
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

    public static boolean needBucket(String process) {
        return needBucketProcesses.contains(process);
    }

    public static boolean needQiniuAuth(String process) {
        return needQiniuAuthProcesses.contains(process);
    }

    public static boolean needTencentAuth(String process) {
        return tenProcesses.contains(process);
    }

    public static boolean needAliyunAuth(String process) {
        return aliProcesses.contains(process);
    }

    public static boolean needAwsS3Auth(String process) {
        return awsProcesses.contains(process);
    }

    public static boolean canBatch(String process) {
        return canBatchProcesses.contains(process);
    }

    public static boolean supportListSource(String process) {
        return supportListSourceProcesses.contains(process);
    }

    public static boolean isDangerous(String process) {
        return dangerousProcesses.contains(process);
    }

//    public static boolean isSupportedProcess(String process) {
//        return processes.contains(process);
//    }
}
