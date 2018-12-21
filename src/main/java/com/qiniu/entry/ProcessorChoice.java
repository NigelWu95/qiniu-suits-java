package com.qiniu.entry;

import com.qiniu.common.Zone;
import com.qiniu.model.parameter.*;
import com.qiniu.service.interfaces.IEntryParam;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.media.QiniuPfop;
import com.qiniu.service.media.QueryAvinfo;
import com.qiniu.service.media.QueryPfopResult;
import com.qiniu.service.process.FileFilter;
import com.qiniu.service.process.FileInfoFilterProcess;
import com.qiniu.service.qoss.*;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ProcessorChoice {

    private List<String> unSupportBatch = new ArrayList<String>(){{
        add("asyncfetch");
        add("avinfo");
        add("pfop");
        add("pfopresult");
        add("qhash");
    }};
    private List<String> canFilterProcesses = new ArrayList<String>(){{
        add("asyncfetch");
        add("status");
        add("type");
        add("copy");
        add("move");
        add("rename");
        add("delete");
        add("stat");
        add("qhash");
        add("lifecycle");
        add("pfop");
        add("avinfo");
    }};
    private IEntryParam entryParam;
    private String process;
    private boolean batch;
    private int retryCount;
    private String resultFileDir;
    private String resultFormat;
    private String resultSeparator;
    private Configuration configuration = new Configuration(Zone.autoZone());

    public ProcessorChoice(IEntryParam entryParam) {
        this.entryParam = entryParam;
        CommonParams commonParams = new CommonParams(entryParam);
        process = commonParams.getProcess();
        batch = commonParams.getProcessBatch();
        if (unSupportBatch.contains(process)) {
            System.out.println(process + " is not support batch operation, it will singly process.");
            batch = false;
        }
        retryCount = commonParams.getRetryCount();
        resultFileDir = commonParams.getResultFileDir();
        resultFormat = commonParams.getResultFormat();
        resultSeparator = commonParams.getResultSeparator();
    }

    public ILineProcess<Map<String, String>> getFileProcessor() throws Exception {

        ListFilterParams listFilterParams = new ListFilterParams(entryParam);
        FileFilter fileFilter = new FileFilter();
        fileFilter.setKeyConditions(listFilterParams.getKeyPrefix(), listFilterParams.getKeySuffix(),
                listFilterParams.getKeyRegex());
        fileFilter.setAntiKeyConditions(listFilterParams.getAntiKeyPrefix(), listFilterParams.getAntiKeySuffix(),
                listFilterParams.getAntiKeyRegex());
        fileFilter.setMimeConditions(listFilterParams.getMime(), listFilterParams.getAntiMime());
        fileFilter.setOtherConditions(listFilterParams.getPutTimeMax(), listFilterParams.getPutTimeMin(),
                listFilterParams.getType());
        ListFieldSaveParams fieldParams = new ListFieldSaveParams(entryParam);
        ILineProcess<Map<String, String>> processor;
        ILineProcess<Map<String, String>> nextProcessor = whichNextProcessor();
        if (canFilterProcesses.contains(process)) {
            if (fileFilter.isValid()) {
                processor = new FileInfoFilterProcess(resultFileDir, resultFormat, resultSeparator, fileFilter,
                        fieldParams.getUsedFields());
                processor.setNextProcessor(nextProcessor);
            } else {
                processor = nextProcessor;
            }
        } else {
            if (fileFilter.isValid()) {
                if (process == null || "".equals(process) || "filter".equals(process)) {
                    processor = new FileInfoFilterProcess(resultFileDir, resultFormat, resultSeparator, fileFilter,
                            fieldParams.getUsedFields());
                } else {
                    System.out.println("this process dons't need filter.");
                    processor = nextProcessor;
                }
            } else {
                if ("filter".equals(process)) {
                    throw new Exception("please set the correct filter conditions.");
                } else {
                    processor = nextProcessor;
                }
            }
        }
        if (processor != null) processor.setRetryCount(retryCount);

        return processor;
    }

    private ILineProcess<Map<String, String>> whichNextProcessor() throws Exception {
        ILineProcess<Map<String, String>> processor = null;
        switch (process) {
            case "status": {
                FileStatusParams fileStatusParams = new FileStatusParams(entryParam);
                String ak = fileStatusParams.getProcessAk();
                String sk = fileStatusParams.getProcessSk();
                processor = new ChangeStatus(Auth.create(ak, sk), configuration, fileStatusParams.getBucket(),
                        fileStatusParams.getTargetStatus(), resultFileDir);
                break;
            }
            case "type": {
                FileTypeParams fileTypeParams = new FileTypeParams(entryParam);
                String ak = fileTypeParams.getProcessAk();
                String sk = fileTypeParams.getProcessSk();
                processor = new ChangeType(Auth.create(ak, sk), configuration, fileTypeParams.getBucket(),
                        fileTypeParams.getTargetType(), resultFileDir);
                break;
            }
            case "lifecycle": {
                LifecycleParams lifecycleParams = new LifecycleParams(entryParam);
                String ak = lifecycleParams.getProcessAk();
                String sk = lifecycleParams.getProcessSk();
                processor = new UpdateLifecycle(Auth.create(ak, sk), configuration, lifecycleParams.getBucket(),
                        lifecycleParams.getDays(), resultFileDir);
                break;
            }
            case "copy": {
                FileCopyParams fileCopyParams = new FileCopyParams(entryParam);
                String ak = fileCopyParams.getProcessAk();
                String sk = fileCopyParams.getProcessSk();
                processor = new CopyFile(Auth.create(ak, sk), configuration, fileCopyParams.getBucket(),
                        fileCopyParams.getTargetBucket(), resultFileDir);
                ((CopyFile) processor).setOptions(fileCopyParams.getKeepKey(), fileCopyParams.getKeyPrefix());
                break;
            }
            case "move":
            case "rename": {
                FileMoveParams fileMoveParams = new FileMoveParams(entryParam);
                String ak = fileMoveParams.getProcessAk();
                String sk = fileMoveParams.getProcessSk();
                processor = new MoveFile(Auth.create(ak, sk), configuration, fileMoveParams.getBucket(),
                        fileMoveParams.getTargetBucket(), resultFileDir);
                ((MoveFile) processor).setOptions(fileMoveParams.getKeyPrefix());
                break;
            }
            case "delete": {
                QossParams qossParams = new QossParams(entryParam);
                String ak = qossParams.getProcessAk();
                String sk = qossParams.getProcessSk();
                processor = new DeleteFile(Auth.create(ak, sk), configuration, qossParams.getBucket(), resultFileDir);
                break;
            }
            case "asyncfetch": {
                AsyncFetchParams asyncFetchParams = new AsyncFetchParams(entryParam);
                String srcAk = asyncFetchParams.getAccessKey();
                String srcSk = asyncFetchParams.getAccessKey();
                String ak = asyncFetchParams.getProcessAk();
                String sk = asyncFetchParams.getProcessSk();
                processor = new AsyncFetch(Auth.create(ak, sk), configuration, asyncFetchParams.getTargetBucket(),
                        asyncFetchParams.getDomain(), resultFileDir);
                ((AsyncFetch) processor).setOptions(asyncFetchParams.getHttps(), asyncFetchParams.getNeedSign() ?
                                Auth.create(srcAk, srcSk) : null, asyncFetchParams.getKeepKey(),
                        asyncFetchParams.getKeyPrefix(), asyncFetchParams.getHashCheck());
                if (asyncFetchParams.hasCustomArgs())
                    ((AsyncFetch) processor).setFetchArgs(asyncFetchParams.getHost(), asyncFetchParams.getCallbackUrl(),
                            asyncFetchParams.getCallbackBody(), asyncFetchParams.getCallbackBodyType(),
                            asyncFetchParams.getCallbackHost(), asyncFetchParams.getFileType(),
                            asyncFetchParams.getIgnoreSameKey());
                break;
            }
            case "avinfo": {
                AvinfoParams avinfoParams = new AvinfoParams(entryParam);
                processor = new QueryAvinfo(avinfoParams.getDomain(), resultFileDir);
                String ak = avinfoParams.getProcessAk();
                String sk = avinfoParams.getProcessSk();
                ((QueryAvinfo) processor).setOptions(avinfoParams.getHttps(), avinfoParams.getNeedSign() ?
                        Auth.create(ak, sk) : null);
                break;
            }
            case "pfop": {
                PfopParams pfopParams = new PfopParams(entryParam);
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
                QhashParams qhashParams = new QhashParams(entryParam);
                processor = new QueryHash(qhashParams.getDomain(), qhashParams.getResultFileDir());
                if (qhashParams.needOptions()) {
                    String ak = qhashParams.getProcessAk();
                    String sk = qhashParams.getProcessSk();
                    ((QueryHash) processor).setOptions(qhashParams.getAlgorithm(),
                            qhashParams.getHttps(), qhashParams.getNeedSign() ? Auth.create(ak, sk) : null);
                }
                break;
            }
            case "stat": {
                QossParams qossParams = new QossParams(entryParam);
                String ak = qossParams.getProcessAk();
                String sk = qossParams.getProcessSk();
                processor = new FileStat(Auth.create(ak, sk), configuration, qossParams.getBucket(),
                        qossParams.getResultFileDir());
                break;
            }
        }
        if (processor != null) {
            processor.setBatch(batch);
            processor.setRetryCount(retryCount);
        }

        return processor;
    }
}
