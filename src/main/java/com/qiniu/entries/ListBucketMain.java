package com.qiniu.entries;

import com.qiniu.common.*;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.model.*;
import com.qiniu.sdk.QiniuAuth;
import com.qiniu.service.impl.*;
import com.qiniu.storage.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ListBucketMain {

    public static void main(String[] args) throws Exception {

        List<String> configFiles = new ArrayList<String>(){{
            add("resources/qiniu.properties");
            add("resources/.qiniu.properties");
        }};
        boolean paramFromConfig = true;
        if (args != null && args.length > 0) {
            if (args[0].startsWith("-config=")) configFiles.add(args[0].split("=")[1]);
            else paramFromConfig = false;
        }
        String configFilePath = null;
        if (paramFromConfig) {
            for (int i = configFiles.size() - 1; i >= 0; i--) {
                File file = new File(configFiles.get(i));
                if (file.exists()) {
                    configFilePath = configFiles.get(i);
                    break;
                }
            }
            if (configFilePath == null) throw new Exception("there is no config file detected.");
            else paramFromConfig = true;
        }

        ListBucketParams listBucketParams = paramFromConfig ? new ListBucketParams(configFilePath) : new ListBucketParams(args);
        String accessKey = listBucketParams.getAccessKey();
        String secretKey = listBucketParams.getSecretKey();
        String bucket = listBucketParams.getBucket();
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
        IOssFileProcess iOssFileProcessor = null;
        QiniuAuth auth = QiniuAuth.create(accessKey, secretKey);
        Configuration configuration = new Configuration(Zone.autoZone());

        switch (process) {
            case "status": {
                FileStatusParams fileStatusParams = paramFromConfig ? new FileStatusParams(configFilePath) : new FileStatusParams(args);
                iOssFileProcessor = new ChangeStatusProcess(auth, configuration, fileStatusParams.getBucket(), fileStatusParams.getTargetStatus(),
                        resultFileDir);
                break;
            }
            case "type": {
                FileTypeParams fileTypeParams = paramFromConfig ? new FileTypeParams(configFilePath) : new FileTypeParams(args);
                iOssFileProcessor = new ChangeTypeProcess(auth, configuration, fileTypeParams.getBucket(), fileTypeParams.getTargetType(),
                        resultFileDir);
                break;
            }
            case "copy": {
                FileCopyParams fileCopyParams = paramFromConfig ? new FileCopyParams(configFilePath) : new FileCopyParams(args);
                accessKey = "".equals(fileCopyParams.getAKey()) ? accessKey : fileCopyParams.getAKey();
                secretKey = "".equals(fileCopyParams.getSKey()) ? secretKey : fileCopyParams.getSKey();
                iOssFileProcessor = new BucketCopyProcess(QiniuAuth.create(accessKey, secretKey), configuration, fileCopyParams.getSourceBucket(),
                        fileCopyParams.getTargetBucket(), fileCopyParams.getTargetKeyPrefix(), resultFileDir);
                break;
            }
            case "lifecycle": {
                LifecycleParams lifecycleParams = paramFromConfig ? new LifecycleParams(configFilePath) : new LifecycleParams(args);
                iOssFileProcessor = new UpdateLifecycleProcess(QiniuAuth.create(accessKey, secretKey), configuration, lifecycleParams.getBucket(),
                        lifecycleParams.getDays(), resultFileDir);
                break;
            }
        }

        ListBucketProcess listBucketProcessor = new ListBucketProcess(auth, configuration, bucket, unitLen, version, resultFileDir,
                customPrefix, antiPrefix);
        if ("check".equals(process)) {
            listBucketProcessor.checkValidPrefix(level, customPrefix, antiPrefix, 3);
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
                listBucketProcessor.processBucket(maxThreads, level, iOssFileProcessor, processBatch, 3);
            } else {
                listBucketProcessor.straightList(customPrefix, "", "", iOssFileProcessor, processBatch, 3);
            }
        }
        if (iOssFileProcessor != null)
            iOssFileProcessor.closeResource();
    }
}