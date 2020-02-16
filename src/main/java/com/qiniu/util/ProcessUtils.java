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
//        add("tenprivate");
//        add("awsprivate");
//        add("s3private");
//        add("aliprivate");
//        add("huaweiprivate");
//        add("baiduprivate");
        add("download");
        add("imagecensor");
        add("videocensor");
        add("cdnrefresh");
        add("cdnprefetch");
        add("refreshquery");
        add("prefetchquery");
        add("fetch");
        add("syncupload");
    }};
    public static Set<String> needToKeyProcesses = new HashSet<String>(){{
        add("copy");
        add("move");
        add("rename");
    }};
    public static Set<String> needFopsProcesses = new HashSet<String>(){{
        add("pfop");
    }};
    public static Set<String> needIdProcesses = new HashSet<String>(){{
        add("pfopresult");
        add("censorresult");
    }};
    public static Set<String> needAvinfoProcesses = new HashSet<String>(){{
        add("pfopcmd");
    }};
    public static Set<String> needFilepathProcesses = new HashSet<String>(){{
        add("qupload");
    }};
    public static Set<String> qiniuProcessesWithBucket = new HashSet<String>(){{
        add("status");
        add("type");
        add("lifecycle");
        add("delete");
        add("copy");
        add("rename");
        add("move");
        add("pfop");
        add("stat");
        add("qupload");
        add("mime");
        add("metadata");
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
        add("mime");
        add("metadata");
        add("cdnrefresh");
        add("cdnprefetch");
        add("refreshquery");
        add("prefetchquery");
    }};
    public static Set<String> tenProcesses = new HashSet<String>(){{
        add("tenprivate");
    }};
    public static Set<String> awsS3Processes = new HashSet<String>(){{
        add("awsprivate");
        add("s3private");
    }};
    public static Set<String> aliProcesses = new HashSet<String>(){{
        add("aliprivate");
    }};
    public static Set<String> huaweiProcesses = new HashSet<String>(){{
        add("huaweiprivate");
    }};
    public static Set<String> baiduProcesses = new HashSet<String>(){{
        add("baiduprivate");
    }};
    public static Set<String> needBucketProcesses = new HashSet<String>(){{
        addAll(qiniuProcessesWithBucket);
        addAll(tenProcesses);
        addAll(awsS3Processes);
        addAll(aliProcesses);
        addAll(huaweiProcesses);
        addAll(baiduProcesses);
    }};
    public static Set<String> needQiniuAuthProcesses = new HashSet<String>(){{
        addAll(qiniuProcessesWithBucket);
        add("mirror");
        add("fetch");
        add("asyncfetch");
        add("syncupload");
        add("privateurl");
        add("imagecensor");
        add("videocensor");
        add("censorresult");
        add("cdnrefresh");
        add("cdnprefetch");
        add("refreshquery");
        add("prefetchquery");
        add("domainsofbucket");
    }};
    public static Set<String> supportStorageSource = new HashSet<String>(){{
        addAll(needBucketProcesses);
        add("qhash");
        add("avinfo");
        add("exportts");
        add("download");
        add("filter");
        add("mirror");
        add("fetch");
        add("asyncfetch");
        add("syncupload");
        add("privateurl");
        add("imagecensor");
        add("videocensor");
        add("censorresult");
        add("cdnrefresh");
        add("cdnprefetch");
        add("refreshquery");
        add("prefetchquery");
        add("publicurl");
    }};
    public static Set<String> dangerousProcesses = new HashSet<String>(){{
        add("status");
        add("lifecycle");
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
    public static Set<String> canPrivateToNextProcesses = new HashSet<String>(){{
        add("exportts");
        add("asyncfetch");
        add("download");
        add("imagecensor");
        add("videocensor");
        add("fetch");
        add("syncupload");
        add("cdnrefresh");
        add("cdnprefetch");
        add("refreshquery");
        add("prefetchquery");
    }};

    public static boolean needUrl(String process) {
        return needUrlProcesses.contains(process);
    }

    public static boolean needToKey(String process) {
        return needToKeyProcesses.contains(process);
    }

    public static boolean needFops(String process) {
        return needFopsProcesses.contains(process);
    }

    public static boolean needId(String process) {
        return needIdProcesses.contains(process);
    }

    public static boolean needAvinfo(String process) {
        return needAvinfoProcesses.contains(process);
    }

    public static boolean needFilepath(String process) {
        return needFilepathProcesses.contains(process);
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
        return awsS3Processes.contains(process);
    }

    public static boolean needHuaweiAuth(String process) {
        return huaweiProcesses.contains(process);
    }

    public static boolean needBaiduAuth(String process) {
        return baiduProcesses.contains(process);
    }

    public static boolean canBatch(String process) {
        return canBatchProcesses.contains(process);
    }

    public static boolean supportStorageSource(String process) {
        return supportStorageSource.contains(process);
    }

    public static boolean isDangerous(String process) {
        return dangerousProcesses.contains(process);
    }

//    public static boolean isSupportedProcess(String process) {
//        return processes.contains(process);
//    }

    public static boolean canPrivateToNext(String process) {
        return canPrivateToNextProcesses.contains(process);
    }
}
