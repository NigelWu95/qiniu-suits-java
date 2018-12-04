package com.qiniu.entry;

import com.qiniu.common.*;
import com.qiniu.model.parameter.ListBucketParams;
import com.qiniu.model.parameter.ListFilterParams;
import com.qiniu.service.convert.FileInfoToMap;
import com.qiniu.service.convert.FileInfoToString;
import com.qiniu.service.datasource.ListBucket;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.service.persistence.ListResultProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.util.List;

public class ListBucketEntry {

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
        Auth auth = Auth.create(accessKey, secretKey);
        Configuration configuration = new Configuration(Zone.autoZone());

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

        ITypeConvert typeConverter = new FileInfoToString(resultFormat, "\t");
        ILineProcess processor = new ListResultProcess(typeConverter, resultFormat, resultFileDir);
        ILineProcess nextProcessor = new ProcessorChoice().getFileProcessor(paramFromConfig, args, configFilePath);
        processor.setNextProcessor(nextProcessor);
        processor.setFilter(listFileFilter, listFileAntiFilter);

        ListBucket listBucket = new ListBucket(auth, configuration, bucket, unitLen, version,
                customPrefix, antiPrefix, 3);
        if (multiStatus) {
            listBucket.concurrentlyList(maxThreads, level, processor);
        } else {
            listBucket.straightlyList(listBucketParams.getMarker(), listBucketParams.getEnd(), processor);
        }

        processor.closeResource();
        if (nextProcessor != null) nextProcessor.closeResource();
    }
}
