package com.qiniu.entry;

import com.qiniu.common.*;
import com.qiniu.model.parameter.ListBucketParams;
import com.qiniu.service.datasource.ListBucket;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.util.List;
import java.util.Map;

public class ListBucketEntry {

    public static void run(boolean paramFromConfig, String[] args, String configFilePath) throws Exception {

        ListBucketParams listBucketParams = paramFromConfig ? new ListBucketParams(configFilePath) : new ListBucketParams(args);
        String accessKey = listBucketParams.getAccessKey();
        String secretKey = listBucketParams.getSecretKey();
        String bucket = listBucketParams.getBucket();
        boolean multiStatus = listBucketParams.getMultiStatus();
        int maxThreads = listBucketParams.getMaxThreads();
        int version = listBucketParams.getVersion();
        int unitLen = listBucketParams.getUnitLen();
        unitLen = (version == 1 && unitLen > 1000) ? unitLen%1000 : unitLen;
        String customPrefix = listBucketParams.getCustomPrefix();
        List<String> antiPrefix = listBucketParams.getAntiPrefix();
        boolean saveTotal = listBucketParams.getSaveTotal();
        String resultFormat = listBucketParams.getResultFormat();
        String resultSeparator = listBucketParams.getResultFormat();
        String resultFileDir = listBucketParams.getResultFileDir();
        Auth auth = Auth.create(accessKey, secretKey);
        Configuration configuration = new Configuration(Zone.autoZone());
        ILineProcess<Map<String, String>> processor = new ProcessorChoice(paramFromConfig, args, configFilePath)
                .getFileProcessor();
        ListBucket listBucket = new ListBucket(auth, configuration, bucket, unitLen, version, maxThreads, customPrefix,
                antiPrefix, 3, resultFileDir);
        listBucket.setSaveTotalOptions(saveTotal, resultFormat, resultSeparator);
        if (multiStatus) {
            listBucket.concurrentlyList(maxThreads, processor);
        } else {
            listBucket.straightlyList(listBucketParams.getMarker(), listBucketParams.getEnd(), processor);
        }
        if (processor != null) processor.closeResource();
    }
}
