package com.qiniu.entry;

import com.qiniu.common.Zone;
import com.qiniu.model.*;
import com.qiniu.service.interfaces.IOssFileProcess;
import com.qiniu.service.oss.CopyFile;
import com.qiniu.service.oss.ChangeStatus;
import com.qiniu.service.oss.ChangeType;
import com.qiniu.service.oss.UpdateLifecycle;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

public class ProcessorChoice {

    public static IOssFileProcess getFileProcessor(boolean paramFromConfig, String[] args, String configFilePath)
            throws Exception {

        CommonParams commonParams = paramFromConfig ? new CommonParams(configFilePath) : new CommonParams(args);
        String accessKey = commonParams.getAccessKey();
        String secretKey = commonParams.getSecretKey();
        String process = commonParams.getProcess();
        boolean batch = commonParams.getProcessBatch();
        String resultFileDir = commonParams.getResultFileDir();
        IOssFileProcess iOssFileProcessor = null;
        Auth auth = Auth.create(accessKey, secretKey);
        Configuration configuration = new Configuration(Zone.autoZone());

        switch (process) {
            case "status": {
                FileStatusParams fileStatusParams = paramFromConfig ?
                        new FileStatusParams(configFilePath) : new FileStatusParams(args);
                iOssFileProcessor = new ChangeStatus(auth, configuration, fileStatusParams.getBucket(),
                        fileStatusParams.getTargetStatus(), resultFileDir, process, batch);
                break;
            }
            case "type": {
                FileTypeParams fileTypeParams = paramFromConfig ?
                        new FileTypeParams(configFilePath) : new FileTypeParams(args);
                iOssFileProcessor = new ChangeType(auth, configuration, fileTypeParams.getBucket(),
                        fileTypeParams.getTargetType(), resultFileDir, process, batch);
                break;
            }
            case "copy": {
                FileCopyParams fileCopyParams = paramFromConfig ?
                        new FileCopyParams(configFilePath) : new FileCopyParams(args);
                accessKey = "".equals(fileCopyParams.getProcessAk()) ? accessKey : fileCopyParams.getProcessAk();
                secretKey = "".equals(fileCopyParams.getProcessSk()) ? secretKey : fileCopyParams.getProcessSk();
                iOssFileProcessor = new CopyFile(Auth.create(accessKey, secretKey), configuration,
                        fileCopyParams.getSourceBucket(), fileCopyParams.getTargetBucket(), fileCopyParams.getKeepKey(),
                        fileCopyParams.getTargetKeyPrefix(), resultFileDir, process, batch);
                break;
            }
            case "lifecycle": {
                LifecycleParams lifecycleParams = paramFromConfig ?
                        new LifecycleParams(configFilePath) : new LifecycleParams(args);
                iOssFileProcessor = new UpdateLifecycle(Auth.create(accessKey, secretKey), configuration,
                        lifecycleParams.getBucket(), lifecycleParams.getDays(), resultFileDir, process, batch);
                break;
            }
        }

        return iOssFileProcessor;
    }
}
