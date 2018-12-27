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

    private IEntryParam entryParam;
    private FileInputParams fileInputParams;
    private String process;
    private int retryCount;
    private String resultPath;
    private String resultFormat;
    private String resultSeparator;
    private Configuration configuration = new Configuration(Zone.autoZone());

    public ProcessorChoice(IEntryParam entryParam) throws IOException {
        this.entryParam = entryParam;
        fileInputParams = new FileInputParams(entryParam);
        process = fileInputParams.getProcess();
        retryCount = fileInputParams.getRetryCount();
        resultPath = fileInputParams.getResultPath();
        resultFormat = fileInputParams.getResultFormat();
        resultSeparator = fileInputParams.getResultSeparator();
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
        ILineProcess<Map<String, String>> processor;
        ILineProcess<Map<String, String>> nextProcessor = whichNextProcessor();
        if (fileFilter.isValid()) {
            processor = new FileInfoFilterProcess(fileFilter, resultPath, resultFormat, resultSeparator,
                    listFilterParams.getRmFields());
            processor.setNextProcessor(nextProcessor);
        } else {
            if ("filter".equals(process)) {
                throw new Exception("please set the correct filter conditions.");
            } else {
                processor = nextProcessor;
            }
        }
        if (processor != null) processor.setRetryCount(retryCount);
        return processor;
    }

    private ILineProcess<Map<String, String>> whichNextProcessor() throws Exception {
        ILineProcess<Map<String, String>> processor = null;
        String ak = "";
        String sk = "";
        try {
            QossParams qossParams = new QossParams(entryParam);
            ak = qossParams.getAccessKey();
            sk = qossParams.getSecretKey();
        } catch (Exception e) {
            List<String> needAkSkProcesses = new ArrayList<String>(){{
                add("status");
                add("type");
                add("lifecycle");
                add("copy");
                add("move");
                add("rename");
                add("delete");
                add("asyncfetch");
                add("pfop");
                add("stat");
                add("privateurl");
            }};
            if (needAkSkProcesses.contains(process)) throw e;
        }
        switch (process) {
            case "status": {
                FileStatusParams fileStatusParams = new FileStatusParams(entryParam);
                processor = new ChangeStatus(Auth.create(ak, sk), configuration, fileStatusParams.getBucket(),
                        fileStatusParams.getTargetStatus(), resultPath);
                break;
            }
            case "type": {
                FileTypeParams fileTypeParams = new FileTypeParams(entryParam);
                processor = new ChangeType(Auth.create(ak, sk), configuration, fileTypeParams.getBucket(),
                        fileTypeParams.getTargetType(), resultPath);
                break;
            }
            case "lifecycle": {
                LifecycleParams lifecycleParams = new LifecycleParams(entryParam);
                processor = new UpdateLifecycle(Auth.create(ak, sk), configuration, lifecycleParams.getBucket(),
                        lifecycleParams.getDays(), resultPath);
                break;
            }
            case "copy": {
                FileCopyParams fileCopyParams = new FileCopyParams(entryParam);
                processor = new CopyFile(Auth.create(ak, sk), configuration, fileCopyParams.getBucket(),
                        fileCopyParams.getTargetBucket(), fileCopyParams.getKeepKey(), fileCopyParams.getKeyPrefix(),
                        resultPath);
                break;
            }
            case "move":
            case "rename": {
                FileMoveParams fileMoveParams = new FileMoveParams(entryParam);
                processor = new MoveFile(Auth.create(ak, sk), configuration, fileMoveParams.getBucket(),
                        fileMoveParams.getToBucket(), fileInputParams.getNewKeyIndex(), fileMoveParams.getKeyPrefix(),
                        fileMoveParams.getForceIfOnlyPrefix(), resultPath);
                break;
            }
            case "delete": {
                QossParams qossParams = new QossParams(entryParam);
                processor = new DeleteFile(Auth.create(ak, sk), configuration, qossParams.getBucket(), resultPath);
                break;
            }
            case "asyncfetch": {
                AsyncFetchParams asyncFetchParams = new AsyncFetchParams(entryParam);
                Auth auth = (asyncFetchParams.getNeedSign()) ? Auth.create(ak, sk) : null;
                processor = new AsyncFetch(Auth.create(ak, sk), configuration, asyncFetchParams.getTargetBucket(),
                        asyncFetchParams.getDomain(), asyncFetchParams.getProtocol(), auth, asyncFetchParams.getKeepKey(),
                        asyncFetchParams.getKeyPrefix(), fileInputParams.getUrlIndex(), resultPath);
                if (asyncFetchParams.hasCustomArgs())
                    ((AsyncFetch) processor).setFetchArgs(fileInputParams.getMd5Index(), asyncFetchParams.getHost(),
                            asyncFetchParams.getCallbackUrl(), asyncFetchParams.getCallbackBody(),
                            asyncFetchParams.getCallbackBodyType(), asyncFetchParams.getCallbackHost(),
                            asyncFetchParams.getFileType(), asyncFetchParams.getIgnoreSameKey());
                break;
            }
            case "avinfo": {
                AvinfoParams avinfoParams = new AvinfoParams(entryParam);
                Auth auth = null;
                if (avinfoParams.getNeedSign()) {
                    ak = avinfoParams.getAccessKey();
                    sk = avinfoParams.getSecretKey();
                    auth = Auth.create(ak, sk);
                }
                processor = new QueryAvinfo(avinfoParams.getDomain(), avinfoParams.getProtocol(),
                        fileInputParams.getUrlIndex(), auth, resultPath);
                break;
            }
            case "pfop": {
                PfopParams pfopParams = new PfopParams(entryParam);
                processor = new QiniuPfop(Auth.create(ak, sk), configuration, pfopParams.getBucket(),
                        pfopParams.getPipeline(), fileInputParams.getFopsIndex(), resultPath);
                break;
            }
            case "pfopresult": {
                processor = new QueryPfopResult(fileInputParams.getPersistentIdIndex(), resultPath);
                break;
            }
            case "qhash": {
                QhashParams qhashParams = new QhashParams(entryParam);
                Auth auth = null;
                if (qhashParams.getNeedSign()) {
                    ak = qhashParams.getAccessKey();
                    sk = qhashParams.getSecretKey();
                    auth = Auth.create(ak, sk);
                }
                processor = new QueryHash(qhashParams.getDomain(), qhashParams.getAlgorithm(), qhashParams.getProtocol(),
                        fileInputParams.getUrlIndex(), auth, qhashParams.getResultPath());
                break;
            }
            case "stat": {
                QossParams qossParams = new QossParams(entryParam);
                processor = new FileStat(Auth.create(ak, sk), configuration, qossParams.getBucket(),
                        qossParams.getResultPath());
                break;
            }
            case "privateurl": {
                PrivateUrlParams privateUrlParams = new PrivateUrlParams(entryParam);
                processor = new PrivateUrl(Auth.create(ak, sk), privateUrlParams.getDomain(), privateUrlParams.getProtocol(),
                        fileInputParams.getUrlIndex(), privateUrlParams.getExpires(), privateUrlParams.getResultPath());
                break;
            }
        }
        if (processor != null) processor.setRetryCount(retryCount);
        return processor;
    }
}
