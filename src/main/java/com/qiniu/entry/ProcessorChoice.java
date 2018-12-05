package com.qiniu.entry;

import com.qiniu.common.Zone;
import com.qiniu.model.parameter.*;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.media.QiniuPfop;
import com.qiniu.service.media.QueryAvinfo;
import com.qiniu.service.media.QueryPfopResult;
import com.qiniu.service.qoss.*;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ProcessorChoice {

    private List<String> unSupportBatch = new ArrayList<>();

    public ProcessorChoice() {
        this.unSupportBatch.add("asyncfetch");
        this.unSupportBatch.add("avinfo");
        this.unSupportBatch.add("pfop");
        this.unSupportBatch.add("pfopresult");
        this.unSupportBatch.add("qhash");
        this.unSupportBatch.add("stat");
    }

    public ILineProcess<Map<String, String>> getFileProcessor(boolean paramFromConfig, String[] args, String configFilePath)
            throws Exception {

        CommonParams commonParams = paramFromConfig ? new CommonParams(configFilePath) : new CommonParams(args);
        String process = commonParams.getProcess();
        boolean batch = commonParams.getProcessBatch();
        if (unSupportBatch.contains(process)) {
            System.out.println(process + " is not support batch operation, it will singly process.");
            batch = false;
        }
        String resultFileDir = commonParams.getResultFileDir();
        ILineProcess<Map<String, String>> processor = null;
        Configuration configuration = new Configuration(Zone.autoZone());

        switch (process) {
            case "status": {
                FileStatusParams fileStatusParams = paramFromConfig ?
                        new FileStatusParams(configFilePath) : new FileStatusParams(args);
                String ak = fileStatusParams.getAccessKey();
                String sk = fileStatusParams.getAccessKey();
                processor = new ChangeStatus(Auth.create(ak, sk), configuration, fileStatusParams.getBucket(),
                        fileStatusParams.getTargetStatus(), resultFileDir);
                break;
            }
            case "type": {
                FileTypeParams fileTypeParams = paramFromConfig ?
                        new FileTypeParams(configFilePath) : new FileTypeParams(args);
                String ak = fileTypeParams.getProcessAk();
                String sk = fileTypeParams.getProcessSk();
                processor = new ChangeType(Auth.create(ak, sk), configuration, fileTypeParams.getBucket(),
                        fileTypeParams.getTargetType(), resultFileDir);
                break;
            }
            case "lifecycle": {
                LifecycleParams lifecycleParams = paramFromConfig ?
                        new LifecycleParams(configFilePath) : new LifecycleParams(args);
                String ak = lifecycleParams.getProcessAk();
                String sk = lifecycleParams.getProcessSk();
                processor = new UpdateLifecycle(Auth.create(ak, sk), configuration, lifecycleParams.getBucket(),
                        lifecycleParams.getDays(), resultFileDir);
                break;
            }
            case "copy": {
                FileCopyParams fileCopyParams = paramFromConfig ?
                        new FileCopyParams(configFilePath) : new FileCopyParams(args);
                String ak = fileCopyParams.getProcessAk();
                String sk = fileCopyParams.getProcessSk();
                processor = new CopyFile(Auth.create(ak, sk), configuration, fileCopyParams.getBucket(),
                        fileCopyParams.getTargetBucket(), resultFileDir);
                ((CopyFile) processor).setOptions(fileCopyParams.getKeepKey(), fileCopyParams.getKeyPrefix());
                break;
            }
            case "delete": {
                QossParams qossParams = paramFromConfig ? new QossParams(configFilePath) : new QossParams(args);
                String ak = qossParams.getProcessAk();
                String sk = qossParams.getProcessSk();
                processor = new DeleteFile(Auth.create(ak, sk), configuration, qossParams.getBucket(), resultFileDir);
                break;
            }
            case "asyncfetch": {
                AsyncFetchParams asyncFetchParams = paramFromConfig ?
                        new AsyncFetchParams(configFilePath) : new AsyncFetchParams(args);
                String ak = asyncFetchParams.getAccessKey();
                String sk = asyncFetchParams.getAccessKey();
                String accessKey = asyncFetchParams.getProcessAk();
                String secretKey = asyncFetchParams.getProcessSk();
                processor = new AsyncFetch(Auth.create(ak, sk), configuration, asyncFetchParams.getTargetBucket(),
                        asyncFetchParams.getDomain(), resultFileDir);
                ((AsyncFetch) processor).setOptions(asyncFetchParams.getHttps(), asyncFetchParams.getNeedSign() ?
                                Auth.create(accessKey, secretKey) : null, asyncFetchParams.getKeepKey(),
                        asyncFetchParams.getKeyPrefix(), asyncFetchParams.getHashCheck());
                if (asyncFetchParams.hasCustomArgs())
                    ((AsyncFetch) processor).setFetchArgs(asyncFetchParams.getHost(), asyncFetchParams.getCallbackUrl(),
                            asyncFetchParams.getCallbackBody(), asyncFetchParams.getCallbackBodyType(),
                            asyncFetchParams.getCallbackHost(), asyncFetchParams.getFileType(),
                            asyncFetchParams.getIgnoreSameKey());
                break;
            }
            case "avinfo": {
                AvinfoParams avinfoParams = paramFromConfig ? new AvinfoParams(configFilePath) : new AvinfoParams(args);
                processor = new QueryAvinfo(avinfoParams.getDomain(), resultFileDir);
                String accessKey = avinfoParams.getProcessAk();
                String secretKey = avinfoParams.getProcessSk();
                ((QueryAvinfo) processor).setOptions(avinfoParams.getHttps(), avinfoParams.getNeedSign() ?
                                Auth.create(accessKey, secretKey) : null);
                break;
            }
            case "pfop": {
                PfopParams pfopParams = paramFromConfig ? new PfopParams(configFilePath) : new PfopParams(args);
                String ak = pfopParams.getProcessAk();
                String sk = pfopParams.getProcessSk();
                processor = new QiniuPfop(Auth.create(ak, sk), configuration, pfopParams.getBucket(),
                        pfopParams.getPipeline(), resultFileDir);
                break;
            }
            case "pfopresult": {
                processor = new QueryPfopResult(resultFileDir);
                break;
            }
            case "qhash": {
                QhashParams qhashParams = paramFromConfig ? new QhashParams(configFilePath) : new QhashParams(args);
                processor = new QueryHash(qhashParams.getDomain(), qhashParams.getResultFileDir());
                break;
            }
            case "stat": {
                QossParams qossParams = paramFromConfig ? new QossParams(configFilePath) : new QossParams(args);
                String ak = qossParams.getProcessAk();
                String sk = qossParams.getProcessSk();
                processor = new FileStat(Auth.create(ak, sk), configuration, qossParams.getBucket(),
                        qossParams.getResultFileDir());
                break;
            }
        }
        if (processor != null) processor.setBatch(batch);

        return processor;
    }
}
