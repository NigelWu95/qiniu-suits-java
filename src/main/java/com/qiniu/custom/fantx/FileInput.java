package com.qiniu.custom.fantx;

import com.qiniu.common.Zone;
import com.qiniu.model.parameter.*;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.media.QiniuPfop;
import com.qiniu.service.media.QueryAvinfo;
import com.qiniu.service.media.QueryPfopResult;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.util.HashMap;
import java.util.Map;

public class FileInput extends com.qiniu.service.datasource.FileInput {

    public static void main(String[] args) throws Exception {

        FileInputParams fileInputParams = new FileInputParams("resources/.qiniu-fantx.properties");
        InfoMapParams infoMapParams = new InfoMapParams("resources/.qiniu-fantx.properties");
        String filePath = fileInputParams.getFilePath();
        String parseType = fileInputParams.getParseType();
        String separator = fileInputParams.getSeparator();
        String resultFileDir = fileInputParams.getResultFileDir();
        boolean saveTotal = false;
        String resultFormat = fileInputParams.getResultFormat();
        String resultSeparator = fileInputParams.getResultFormat();
        int maxThreads = fileInputParams.getMaxThreads();
        int unitLen = fileInputParams.getUnitLen();
        String sourceFilePath = System.getProperty("user.dir") + System.getProperty("file.separator") + filePath;

        Configuration configuration = new Configuration(Zone.autoZone());
//        AvinfoParams avinfoParams = new AvinfoParams("resources/.qiniu-fantx.properties");
        // parse avinfo from files.
//        ILineProcess processor = new AvinfoProcess(avinfoParams.getBucket(), resultFileDir);
        // query persistent id and parse
        ILineProcess<Map<String, String>> processor = new QueryPfopResult(resultFileDir);
        // filter pfop result
//        ILineProcess<Map<String, String>> processor = new PfopResultProcess(resultFileDir);
        // copy file
//        FileCopyParams fileCopyParams = new FileCopyParams("resources/.qiniu-fantx.properties");
//        String ak = fileCopyParams.getProcessAk();
//        String sk = fileCopyParams.getProcessSk();
//        ILineProcess processor = new FileCopy(Auth.create(ak, sk), configuration, fileCopyParams.getBucket(),
//                fileCopyParams.getTargetBucket(), resultFileDir);
//        ILineProcess processor = new QueryAvinfo(avinfoParams.getDomain(), resultFileDir);
        // pfop request
//        PfopParams pfopParams = new PfopParams("resources/.qiniu-fantx.properties");
//        String ak = pfopParams.getProcessAk();
//        String sk = pfopParams.getProcessSk();
//        ILineProcess<Map<String, String>> processor = new QiniuPfop(Auth.create(ak, sk), configuration,
//                pfopParams.getBucket(), pfopParams.getPipeline(), resultFileDir);
//        ILineProcess<Map<String, String>> processor = new PfopProcess(Auth.create(ak, sk), configuration,
//                pfopParams.getBucket(), pfopParams.getPipeline(), resultFileDir);

        Map<String, String> infoIndexMap = new HashMap<>();
        infoIndexMap.put(fileInputParams.getKeyIndex(), "key");
        infoIndexMap.put(fileInputParams.getHashIndex(), "hash");
        infoIndexMap.put(fileInputParams.getFsizeIndex(), "fsize");
        infoIndexMap.put(fileInputParams.getPutTimeIndex(), "putTime");
        infoIndexMap.put(fileInputParams.getMimeTypeIndex(), "mimeType");
        infoIndexMap.put(fileInputParams.getEndUserIndex(), "endUser");
        infoIndexMap.put(fileInputParams.getTypeIndex(), "type");
        infoIndexMap.put(fileInputParams.getStatusIndex(), "status");
        infoIndexMap.put(fileInputParams.getMd5Index(), "md5");
        infoIndexMap.put(fileInputParams.getFopsIndex(), "fops");
        infoIndexMap.put(fileInputParams.getPersistentIdIndex(), "persistentId");
        FileInput fileInput = new FileInput(parseType, separator, infoIndexMap, 3, unitLen, resultFileDir);
        fileInput.process(maxThreads, sourceFilePath, processor);
        processor.closeResource();
    }

    public FileInput(String parseType, String separator, Map<String, String> infoIndexMap, int retryCount, int unitLen,
                     String resultFileDir) {
        super(parseType, separator, infoIndexMap, retryCount, unitLen, resultFileDir);
    }
}
