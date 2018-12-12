package com.qiniu.entry;

import com.qiniu.common.*;
import com.qiniu.model.parameter.ListBucketParams;
import com.qiniu.model.parameter.ListFilterParams;
import com.qiniu.service.datasource.ListBucket;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.process.FileFilter;
import com.qiniu.service.process.FileInfoFilterProcess;
import com.qiniu.service.process.ListResultProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;

import java.util.List;
import java.util.Map;

public class ListBucketEntry {

    public static void run(boolean paramFromConfig, String[] args, String configFilePath) throws Exception {

        ListBucketParams listBucketParams = paramFromConfig ? new ListBucketParams(configFilePath) : new ListBucketParams(args);
        String accessKey = listBucketParams.getAccessKey();
        String secretKey = listBucketParams.getSecretKey();
        String bucket = listBucketParams.getBucket();
        String resultFileDir = listBucketParams.getResultFileDir();
        String resultFormat = listBucketParams.getResultFormat();
        String resultSeparator = "\t";
        boolean multiStatus = listBucketParams.getMultiStatus();
        int maxThreads = listBucketParams.getMaxThreads();
        int version = listBucketParams.getVersion();
        int level = listBucketParams.getLevel();
        int unitLen = listBucketParams.getUnitLen();
        unitLen = (version == 1 && unitLen > 1000) ? unitLen%1000 : unitLen;
        String customPrefix = listBucketParams.getCustomPrefix();
        List<String> antiPrefix = listBucketParams.getAntiPrefix();
        Auth auth = Auth.create(accessKey, secretKey);
        Configuration configuration = new Configuration(Zone.autoZone());
        ILineProcess<FileInfo> processor = new ListResultProcess(resultFileDir);

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
        ILineProcess<Map<String, String>> nextProcessor;
        ILineProcess<Map<String, String>> lastProcessor = new ProcessorChoice().getFileProcessor(paramFromConfig, args,
                configFilePath);
        if (fileFilter.isValid()) {
            nextProcessor = new FileInfoFilterProcess(resultFormat, null, resultFileDir, fileFilter);
            nextProcessor.setNextProcessor(lastProcessor);
        } else {
            nextProcessor = lastProcessor;
        }
        processor.setNextProcessor(nextProcessor);

        ListBucket listBucket = new ListBucket(auth, configuration, bucket, unitLen, version,
                customPrefix, antiPrefix, 3, resultFileDir);
        listBucket.setSaveTotalOptions(resultFormat, resultSeparator);
        if (multiStatus) {
            listBucket.concurrentlyList(maxThreads, level, processor);
        } else {
            listBucket.straightlyList(listBucketParams.getMarker(), listBucketParams.getEnd(), processor);
        }

        processor.closeResource();
    }
}
