package com.qiniu.entry;

import com.qiniu.common.*;
import com.qiniu.model.parameter.ListBucketParams;
import com.qiniu.model.parameter.ListFilterParams;
import com.qiniu.service.datasource.ListBucket;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.process.FileFilter;
import com.qiniu.service.process.FileInfoFilterProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.util.ArrayList;
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
        int level = listBucketParams.getLevel();
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

        ListFilterParams listFilterParams = paramFromConfig ?
                new ListFilterParams(configFilePath) : new ListFilterParams(args);
        FileFilter fileFilter = new FileFilter();
        fileFilter.setKeyConditions(listFilterParams.getKeyPrefix(), listFilterParams.getKeySuffix(),
                listFilterParams.getKeyRegex());
        fileFilter.setAntiKeyConditions(listFilterParams.getAntiKeyPrefix(), listFilterParams.getAntiKeySuffix(),
                listFilterParams.getAntiKeyRegex());
        fileFilter.setMimeConditions(listFilterParams.getMime(), listFilterParams.getAntiMime());
        fileFilter.setOtherConditions(listFilterParams.getPutTimeMax(), listFilterParams.getPutTimeMin(),
                listFilterParams.getType());

        List<String> canFilterProcesses = new ArrayList<String>(){{
            add("asyncfetch");
            add("status");
            add("type");
            add("copy");
            add("delete");
            add("stat");
            add("qhash");
            add("lifecycle");
            add("pfop");
            add("avinfo");
        }};

        ILineProcess<Map<String, String>> processor;
        if (canFilterProcesses.contains(listBucketParams.getProcess())) {
            ILineProcess<Map<String, String>> nextProcessor = new ProcessorChoice().getFileProcessor(paramFromConfig,
                    args, configFilePath);
            if (!fileFilter.isValid()) {
                processor = nextProcessor;
            } else {
                processor = new FileInfoFilterProcess(resultFileDir, resultFormat, resultSeparator, fileFilter);
                processor.setNextProcessor(nextProcessor);
            }
        } else {
            if ("filter".equals(listBucketParams.getProcess())) {
                if (fileFilter.isValid()) {
                    processor = new FileInfoFilterProcess(resultFileDir, resultFormat, resultSeparator, fileFilter);
                } else {
                    throw new Exception("please set the correct filter conditions.");
                }
            } else {
                System.out.println("this process dons't need filter.");
                processor = new ProcessorChoice().getFileProcessor(paramFromConfig, args, configFilePath);
            }
        }

        ListBucket listBucket = new ListBucket(auth, configuration, bucket, unitLen, version,
                customPrefix, antiPrefix, 3, resultFileDir);
        listBucket.setSaveTotalOptions(saveTotal, resultFormat, resultSeparator);
        if (multiStatus) {
            listBucket.concurrentlyList(maxThreads, level, processor);
        } else {
            listBucket.straightlyList(listBucketParams.getMarker(), listBucketParams.getEnd(), processor);
        }

        processor.closeResource();
    }
}
