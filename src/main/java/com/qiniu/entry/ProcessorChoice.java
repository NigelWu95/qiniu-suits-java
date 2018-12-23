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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ProcessorChoice {

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
    private int retryCount;
    private String resultPath;
    private String resultFormat;
    private String resultSeparator;
    private Configuration configuration = new Configuration(Zone.autoZone());

    public ProcessorChoice(IEntryParam entryParam) throws IOException {
        this.entryParam = entryParam;
        CommonParams commonParams = new CommonParams(entryParam);
        process = commonParams.getProcess();
        retryCount = commonParams.getRetryCount();
        resultPath = commonParams.getResultPath();
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
                processor = new FileInfoFilterProcess(resultPath, resultFormat, resultSeparator, fileFilter,
                        fieldParams.getUsedFields());
                processor.setNextProcessor(nextProcessor);
            } else {
                processor = nextProcessor;
            }
        } else {
            if (fileFilter.isValid()) {
                if (process == null || "".equals(process) || "filter".equals(process)) {
                    processor = new FileInfoFilterProcess(resultPath, resultFormat, resultSeparator, fileFilter,
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
                        fileStatusParams.getTargetStatus(), resultPath);
                break;
            }
            case "type": {
                FileTypeParams fileTypeParams = new FileTypeParams(entryParam);
                String ak = fileTypeParams.getProcessAk();
                String sk = fileTypeParams.getProcessSk();
                processor = new ChangeType(Auth.create(ak, sk), configuration, fileTypeParams.getBucket(),
                        fileTypeParams.getTargetType(), resultPath);
                break;
            }
            case "lifecycle": {
                LifecycleParams lifecycleParams = new LifecycleParams(entryParam);
                String ak = lifecycleParams.getProcessAk();
                String sk = lifecycleParams.getProcessSk();
                processor = new UpdateLifecycle(Auth.create(ak, sk), configuration, lifecycleParams.getBucket(),
                        lifecycleParams.getDays(), resultPath);
                break;
            }
            case "copy": {
                FileCopyParams fileCopyParams = new FileCopyParams(entryParam);
                String ak = fileCopyParams.getProcessAk();
                String sk = fileCopyParams.getProcessSk();
                processor = new CopyFile(Auth.create(ak, sk), configuration, fileCopyParams.getBucket(),
                        fileCopyParams.getTargetBucket(), resultPath);
                ((CopyFile) processor).setOptions(fileCopyParams.getKeepKey(), fileCopyParams.getKeyPrefix());
                break;
            }
            case "move":
            case "rename": {
                FileMoveParams fileMoveParams = new FileMoveParams(entryParam);
                String ak = fileMoveParams.getProcessAk();
                String sk = fileMoveParams.getProcessSk();
                processor = new MoveFile(Auth.create(ak, sk), configuration, fileMoveParams.getBucket(),
                        fileMoveParams.getTargetBucket(), resultPath);
                ((MoveFile) processor).setOptions(fileMoveParams.getKeyPrefix());
                break;
            }
            case "delete": {
                QossParams qossParams = new QossParams(entryParam);
                String ak = qossParams.getProcessAk();
                String sk = qossParams.getProcessSk();
                processor = new DeleteFile(Auth.create(ak, sk), configuration, qossParams.getBucket(), resultPath);
                break;
            }
            case "asyncfetch": {
                AsyncFetchParams asyncFetchParams = new AsyncFetchParams(entryParam);
                String srcAk = asyncFetchParams.getAccessKey();
                String srcSk = asyncFetchParams.getAccessKey();
                String ak = asyncFetchParams.getProcessAk();
                String sk = asyncFetchParams.getProcessSk();
                processor = new AsyncFetch(Auth.create(ak, sk), configuration, asyncFetchParams.getTargetBucket(),
                        asyncFetchParams.getDomain(), resultPath);
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
                processor = new QueryAvinfo(avinfoParams.getDomain(), resultPath);
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
                        pfopParams.getPipeline(), resultPath);
                break;
            }
            case "pfopresult": {
                processor = new QueryPfopResult(resultPath);
                break;
            }
            case "qhash": {
                QhashParams qhashParams = new QhashParams(entryParam);
                processor = new QueryHash(qhashParams.getDomain(), qhashParams.getResultPath());
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
                        qossParams.getResultPath());
                break;
            }
        }
        if (processor != null) processor.setRetryCount(retryCount);
        return processor;
    }
}
