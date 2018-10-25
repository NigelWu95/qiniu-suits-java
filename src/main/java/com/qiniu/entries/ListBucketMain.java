package com.qiniu.entries;

import com.qiniu.common.*;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.model.*;
import com.qiniu.sdk.QiniuAuth;
import com.qiniu.service.impl.*;
import com.qiniu.storage.Configuration;

import java.util.List;

public class ListBucketMain {

    public static void main(String[] args) throws Exception {

        String configFile = ".qiniu.properties";
        boolean paramFromConfig = (args == null || args.length == 0);
        ListBucketParams listBucketParams = paramFromConfig ? new ListBucketParams(configFile) : new ListBucketParams(args);
        String accessKey = listBucketParams.getAccessKey();
        String secretKey = listBucketParams.getSecretKey();
        String bucket = listBucketParams.getBucket();
        String resultFileDir = listBucketParams.getResultFileDir();
        int maxThreads = listBucketParams.getMaxThreads();
        int version = listBucketParams.getVersion();
        int level = listBucketParams.getLevel();
        int unitLen = listBucketParams.getUnitLen();
        unitLen = (version == 1 && unitLen > 1000) ? unitLen%1000 : unitLen;
        String customPrefix = listBucketParams.getCustomPrefix();
        List<String> antiPrefix = listBucketParams.getAntiPrefix();
        String process = listBucketParams.getProcess();
        boolean processBatch = listBucketParams.getProcessBatch();
        IOssFileProcess iOssFileProcessor = null;
        QiniuAuth auth = QiniuAuth.create(accessKey, secretKey);
        Configuration configuration = new Configuration(Zone.autoZone());

        switch (process) {
            case "status": {
                FileStatusParams fileStatusParams = paramFromConfig ? new FileStatusParams(configFile) : new FileStatusParams(args);
                iOssFileProcessor = new ChangeStatusProcess(auth, configuration, fileStatusParams.getBucket(), fileStatusParams.getTargetStatus(),
                        resultFileDir);
                break;
            }
            case "type": {
                FileTypeParams fileTypeParams = paramFromConfig ? new FileTypeParams(configFile) : new FileTypeParams(args);
                iOssFileProcessor = new ChangeTypeProcess(auth, configuration, fileTypeParams.getBucket(), fileTypeParams.getTargetType(),
                        resultFileDir);
                break;
            }
            case "copy": {
                FileCopyParams fileCopyParams = paramFromConfig ? new FileCopyParams(configFile) : new FileCopyParams(args);
                accessKey = "".equals(fileCopyParams.getAKey()) ? accessKey : fileCopyParams.getAKey();
                secretKey = "".equals(fileCopyParams.getSKey()) ? secretKey : fileCopyParams.getSKey();
                iOssFileProcessor = new BucketCopyProcess(QiniuAuth.create(accessKey, secretKey), configuration, fileCopyParams.getSourceBucket(),
                        fileCopyParams.getTargetBucket(), fileCopyParams.getTargetKeyPrefix(), resultFileDir);
                break;
            }
            case "lifecycle": {
                LifecycleParams lifecycleParams = paramFromConfig ? new LifecycleParams(configFile) : new LifecycleParams(args);
                iOssFileProcessor = new UpdateLifecycleProcess(QiniuAuth.create(accessKey, secretKey), configuration, lifecycleParams.getBucket(),
                        lifecycleParams.getDays(), resultFileDir);
                break;
            }
        }

        ListBucketProcess listBucketProcessor = new ListBucketProcess(auth, configuration, bucket, unitLen, version, resultFileDir,
                customPrefix, antiPrefix);
        if ("check".equals(process)) {
            listBucketProcessor.checkValidPrefix(level, customPrefix, antiPrefix, "check", 3);
        } else {
            ListFilterParams listFilterParams = paramFromConfig ? new ListFilterParams(configFile) : new ListFilterParams(args);
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
            listBucketProcessor.processBucket(maxThreads, level, iOssFileProcessor, processBatch, 3);
        }
        if (iOssFileProcessor != null)
            iOssFileProcessor.closeResource();
    }
}