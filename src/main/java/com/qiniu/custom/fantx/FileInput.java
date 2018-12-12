package com.qiniu.custom.fantx;

import com.qiniu.common.Zone;
import com.qiniu.model.parameter.*;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.media.QiniuPfop;
import com.qiniu.service.media.QueryAvinfo;
import com.qiniu.service.media.QueryPfopResult;
import com.qiniu.service.process.FileInputResultProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.util.Map;

public class FileInput extends com.qiniu.service.datasource.FileInput {

    public static void main(String[] args) throws Exception {

        FileInputParams fileInputParams = new FileInputParams("resources/.qiniu-fantx.properties");
        InfoMapParams infoMapParams = new InfoMapParams("resources/.qiniu-fantx.properties");
        String filePath = fileInputParams.getFilePath();
        String parserType = fileInputParams.getParserType();
        String inputSeparator = fileInputParams.getSeparator();
        String resultFormat = fileInputParams.getResultFormat();
        String resultSeparator = fileInputParams.getResultFormat();
        String resultFileDir = fileInputParams.getResultFileDir();
        boolean saveTotal = false;
        int maxThreads = fileInputParams.getMaxThreads();
        int unitLen = fileInputParams.getUnitLen();
        String sourceFilePath = System.getProperty("user.dir") + System.getProperty("file.separator") + filePath;
        ILineProcess<String> inputResultProcessor = new FileInputResultProcess(parserType, inputSeparator, infoMapParams,
                resultFormat, resultSeparator, resultFileDir, saveTotal);

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
        inputResultProcessor.setNextProcessor(processor);
        FileInput fileInput = new FileInput(unitLen);
        fileInput.process(maxThreads, sourceFilePath, inputResultProcessor);
        inputResultProcessor.closeResource();
    }

    public FileInput(int unitLen) {
        super(unitLen);
    }
}
