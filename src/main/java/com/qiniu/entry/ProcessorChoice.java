package com.qiniu.entry;

import com.qiniu.common.Zone;
import com.qiniu.model.*;
import com.qiniu.service.interfaces.IOssFileProcess;
import com.qiniu.service.oss.*;
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
        IOssFileProcess processor = null;
        Auth auth = Auth.create(accessKey, secretKey);
        Configuration configuration = new Configuration(Zone.autoZone());

        switch (process) {
            case "status": {
                FileStatusParams fileStatusParams = paramFromConfig ?
                        new FileStatusParams(configFilePath) : new FileStatusParams(args);
                processor = new ChangeStatus(auth, configuration, fileStatusParams.getBucket(),
                        fileStatusParams.getTargetStatus(), resultFileDir);
                break;
            }
            case "type": {
                FileTypeParams fileTypeParams = paramFromConfig ?
                        new FileTypeParams(configFilePath) : new FileTypeParams(args);
                processor = new ChangeType(auth, configuration, fileTypeParams.getBucket(),
                        fileTypeParams.getTargetType(), resultFileDir);
                break;
            }
            case "copy": {
                FileCopyParams fileCopyParams = paramFromConfig ?
                        new FileCopyParams(configFilePath) : new FileCopyParams(args);
                accessKey = "".equals(fileCopyParams.getProcessAk()) ? accessKey : fileCopyParams.getProcessAk();
                secretKey = "".equals(fileCopyParams.getProcessSk()) ? secretKey : fileCopyParams.getProcessSk();
                processor = new CopyFile(Auth.create(accessKey, secretKey), configuration,
                        fileCopyParams.getSourceBucket(), fileCopyParams.getTargetBucket(), resultFileDir);
                ((CopyFile) processor).setCopyOptions(fileCopyParams.getKeepKey(), fileCopyParams.getTargetKeyPrefix());
                break;
            }
            case "lifecycle": {
                LifecycleParams lifecycleParams = paramFromConfig ?
                        new LifecycleParams(configFilePath) : new LifecycleParams(args);
                processor = new UpdateLifecycle(Auth.create(accessKey, secretKey), configuration,
                        lifecycleParams.getBucket(), lifecycleParams.getDays(), resultFileDir);
                break;
            }
            case "asyncfetch": {
                AsyncFetchParams asyncFetchParams = paramFromConfig ?
                        new AsyncFetchParams(configFilePath) : new AsyncFetchParams(args);
                accessKey = "".equals(asyncFetchParams.getProcessAk()) ? accessKey : asyncFetchParams.getProcessAk();
                secretKey = "".equals(asyncFetchParams.getProcessSk()) ? secretKey : asyncFetchParams.getProcessSk();
                processor = new AsyncFetch(Auth.create(accessKey, secretKey), configuration, "", "",
                        resultFileDir);
                break;
            }
        }
        if (processor != null) processor.setBatch(batch);

        return processor;
    }
}
