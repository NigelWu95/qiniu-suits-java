package com.qiniu.entries;

import com.qiniu.common.*;
import com.qiniu.interfaces.IBucketProcess;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.model.*;
import com.qiniu.service.impl.*;
import com.qiniu.storage.Configuration;

public class ListBucketMain {

    public static void main(String[] args) throws Exception {

        String configFile = ".qiniu.properties";
        boolean paramFromConfig = (args == null || args.length == 0);
        ListBucketParams listBucketParams = paramFromConfig ?
                new ListBucketParams(configFile) : new ListBucketParams(args);
        String accessKey = listBucketParams.getAccessKey();
        String secretKey = listBucketParams.getSecretKey();
        String bucket = listBucketParams.getBucket();
        String resultFileDir = listBucketParams.getResultFileDir();
        int maxThreads = listBucketParams.getMaxThreads();
        int version = listBucketParams.getVersion();
        int level = listBucketParams.getLevel();
        boolean enabledEndFile = listBucketParams.getEnabledEndFile();
        int unitLen = listBucketParams.getUnitLen();
        unitLen = (version == 1 && unitLen > 1000) ? unitLen%1000 : unitLen;
        String customPrefix = listBucketParams.getCustomPrefix();
        String process = listBucketParams.getProcess();
        boolean processBatch = listBucketParams.getProcessBatch();
        IOssFileProcess iOssFileProcessor = null;
        QiniuAuth auth = QiniuAuth.create(accessKey, secretKey);
        Configuration configuration = new Configuration(Zone.autoZone());

        switch (process) {
            // isBiggerThan 标志为 true 时，在 pointTime 时间点之前的记录进行处理，isBiggerThan 标志为 false 时，在 pointTime 时间点之后的记录进行处理。
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
            case "filter": {
                iOssFileProcessor = new ListFilterProcess(resultFileDir);
                break;
            }
        }

        IBucketProcess listBucketProcessor = new ListBucketProcess(auth, configuration, bucket, resultFileDir);
        if ("check".equals(process)) {
            ((ListBucketProcess) listBucketProcessor).getDelimitedFileMap(version, level, customPrefix, "delimiter",
                    null, 3);
        } else
            listBucketProcessor.processBucket(version, maxThreads, level, unitLen, enabledEndFile, customPrefix,
                    iOssFileProcessor, processBatch);

        if (iOssFileProcessor != null)
            iOssFileProcessor.closeResource();
    }
}