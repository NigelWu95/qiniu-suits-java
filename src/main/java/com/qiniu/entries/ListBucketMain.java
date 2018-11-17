package com.qiniu.entries;

import com.qiniu.common.*;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.model.*;
import com.qiniu.service.impl.*;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.util.List;

public class ListBucketMain {

    public static void runMain(boolean paramFromConfig, String[] args, String configFilePath) throws Exception {

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
        boolean processBatch = listBucketParams.getProcessBatch();
        IOssFileProcess iOssFileProcessor = ProcessChoice.getFileProcessor(paramFromConfig, args, configFilePath);
        Auth auth = Auth.create(accessKey, secretKey);
        Configuration configuration = new Configuration(Zone.autoZone());
        ListBucketProcess listBucketProcessor = new ListBucketProcess(auth, configuration, bucket, unitLen, version,
                customPrefix, antiPrefix, 3);
        listBucketProcessor.setResultParams(resultFormat, resultFileDir);
        if ("check".equals(process)) {
            listBucketProcessor.checkValidPrefix(level, customPrefix, antiPrefix);
        } else {
            ListFilterParams listFilterParams = paramFromConfig ? new ListFilterParams(configFilePath) : new ListFilterParams(args);
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
            listBucketProcessor.setFilter(listFileFilter, listFileAntiFilter);
            if (multiStatus) {
                listBucketProcessor.processBucket(maxThreads, level, iOssFileProcessor, processBatch);
            } else {
                listBucketProcessor.straightList(customPrefix, "", "", iOssFileProcessor, processBatch);
            }
        }
        if (iOssFileProcessor != null) iOssFileProcessor.closeResource();
    }
}
