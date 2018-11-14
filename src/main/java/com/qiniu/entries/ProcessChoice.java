package com.qiniu.entries;

import com.qiniu.common.Zone;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.model.*;
import com.qiniu.service.impl.BucketCopyProcess;
import com.qiniu.service.impl.ChangeStatusProcess;
import com.qiniu.service.impl.ChangeTypeProcess;
import com.qiniu.service.impl.UpdateLifecycleProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

public class ProcessChoice {

    public static IOssFileProcess getFileProcessor(boolean paramFromConfig, String[] args, String configFilePath) throws Exception {

        SourceFileParams sourceFileParams = paramFromConfig ? new SourceFileParams(configFilePath) : new SourceFileParams(args);
        String accessKey = sourceFileParams.getAccessKey();
        String secretKey = sourceFileParams.getSecretKey();
        String process = sourceFileParams.getProcess();
        String resultFileDir = sourceFileParams.getResultFileDir();
        IOssFileProcess iOssFileProcessor = null;
        Auth auth = Auth.create(accessKey, secretKey);
        Configuration configuration = new Configuration(Zone.autoZone());

        switch (process) {
            case "status": {
                FileStatusParams fileStatusParams = paramFromConfig ? new FileStatusParams(configFilePath) : new FileStatusParams(args);
                iOssFileProcessor = new ChangeStatusProcess(auth, configuration, fileStatusParams.getBucket(),
                        fileStatusParams.getTargetStatus(), resultFileDir, process);
                break;
            }
            case "type": {
                FileTypeParams fileTypeParams = paramFromConfig ? new FileTypeParams(configFilePath) : new FileTypeParams(args);
                iOssFileProcessor = new ChangeTypeProcess(auth, configuration, fileTypeParams.getBucket(),
                        fileTypeParams.getTargetType(), resultFileDir, process);
                break;
            }
            case "copy": {
                FileCopyParams fileCopyParams = paramFromConfig ? new FileCopyParams(configFilePath) : new FileCopyParams(args);
                accessKey = "".equals(fileCopyParams.getAKey()) ? accessKey : fileCopyParams.getAKey();
                secretKey = "".equals(fileCopyParams.getSKey()) ? secretKey : fileCopyParams.getSKey();
                iOssFileProcessor = new BucketCopyProcess(Auth.create(accessKey, secretKey), configuration,
                        fileCopyParams.getSourceBucket(), fileCopyParams.getTargetBucket(),
                        fileCopyParams.getTargetKeyPrefix(), resultFileDir, process);
                break;
            }
            case "lifecycle": {
                LifecycleParams lifecycleParams = paramFromConfig ? new LifecycleParams(configFilePath) : new LifecycleParams(args);
                iOssFileProcessor = new UpdateLifecycleProcess(Auth.create(accessKey, secretKey), configuration,
                        lifecycleParams.getBucket(), lifecycleParams.getDays(), resultFileDir, process);
                break;
            }
        }

        return iOssFileProcessor;
    }
}