package com.qiniu.entry;

import com.qiniu.common.*;
import com.qiniu.model.*;
import com.qiniu.service.datasource.ListBucket;
import com.qiniu.service.interfaces.IOssFileProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.util.List;

public class ListBucketProcess {

    public static void run(boolean paramFromConfig, String[] args, String configFilePath) throws Exception {

        ListBucketParams listBucketParams = paramFromConfig ? new ListBucketParams(configFilePath) : new ListBucketParams(args);
        String accessKey = listBucketParams.getAccessKey();
        String secretKey = listBucketParams.getSecretKey();
        String bucket = listBucketParams.getBucket();
        String resultFormat = listBucketParams.getResultFormat();
        String resultFileDir = listBucketParams.getResultFileDir();
        boolean multiStatus = listBucketParams.getMultiStatus();
        int maxThreads = listBucketParams.getMaxThreads();
        int version = listBucketParams.getVersion();
        int level = listBucketParams.getLevel();
        int unitLen = listBucketParams.getUnitLen();
        unitLen = (version == 1 && unitLen > 1000) ? unitLen%1000 : unitLen;
        String customPrefix = listBucketParams.getCustomPrefix();
        List<String> antiPrefix = listBucketParams.getAntiPrefix();
        String process = listBucketParams.getProcess();
        IOssFileProcess iOssFileProcessor = ProcessorChoice.getFileProcessor(paramFromConfig, args, configFilePath);
        Auth auth = Auth.create(accessKey, secretKey);
        Configuration configuration = new Configuration(Zone.autoZone());
        ListBucket listBucket = new ListBucket(auth, configuration, bucket, unitLen, version,
                customPrefix, antiPrefix, 3);
        listBucket.setResultParams(resultFormat, resultFileDir);
        if ("check".equals(process)) {
            listBucket.checkValidPrefix(level);
        } else {
            ListFilterParams listFilterParams = paramFromConfig ?
                    new ListFilterParams(configFilePath) : new ListFilterParams(args);
            ListFileFilter listFileFilter = new ListFileFilter();
            ListFileAntiFilter listFileAntiFilter = new ListFileAntiFilter();
            listFileFilter.setKeyPrefix(listFilterParams.getKeyPrefix());
            listFileFilter.setKeySuffix(listFilterParams.getKeySuffix());
            listFileFilter.setKeyRegex(listFilterParams.getKeyRegex());
            listFileFilter.setPutTimeMax(listFilterParams.getPutTimeMax());
            listFileFilter.setPutTimeMin(listFilterParams.getPutTimeMin());
            listFileFilter.setMime(listFilterParams.getMime());
            listFileFilter.setType(listFilterParams.getType());
            listFileAntiFilter.setKeyPrefix(listFilterParams.getAntiKeyPrefix());
            listFileAntiFilter.setKeySuffix(listFilterParams.getAntiKeySuffix());
            listFileAntiFilter.setKeyRegex(listFilterParams.getAntiKeyRegex());
            listFileAntiFilter.setMime(listFilterParams.getAntiMime());
            listBucket.setFilter(listFileFilter, listFileAntiFilter);
            if (multiStatus) {
                listBucket.concurrentlyList(maxThreads, level, iOssFileProcessor);
            } else {
                listBucket.straightlyList("", "", iOssFileProcessor);
            }
        }
        if (iOssFileProcessor != null) iOssFileProcessor.closeResource();
    }
}
