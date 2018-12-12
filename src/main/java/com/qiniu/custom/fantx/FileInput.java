package com.qiniu.custom.fantx;

import com.qiniu.common.Zone;
import com.qiniu.model.parameter.AvinfoParams;
import com.qiniu.model.parameter.FileCopyParams;
import com.qiniu.model.parameter.FileInputParams;
import com.qiniu.model.parameter.PfopParams;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.media.QiniuPfop;
import com.qiniu.service.media.QueryAvinfo;
import com.qiniu.service.media.QueryPfopResult;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.util.Map;

public class FileInput extends com.qiniu.service.datasource.FileInput {

    public static void main(String[] args) throws Exception {

        FileInputParams fileInputParams = new FileInputParams("resources/.qiniu-fantx.properties");
        String filePath = fileInputParams.getFilePath();
        String separator = fileInputParams.getSeparator();
        int keyIndex = fileInputParams.getKeyIndex();
        int maxThreads = fileInputParams.getMaxThreads();
        int unitLen = fileInputParams.getUnitLen();
        String sourceFilePath = System.getProperty("user.dir") + System.getProperty("file.separator") + filePath;
        String resultFileDir = fileInputParams.getResultFileDir();
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
        FileInput fileInput = new FileInput(separator, keyIndex, unitLen);
        fileInput.process(maxThreads, sourceFilePath, processor);
        processor.closeResource();
    }

    private String separator;
    private int keyIndex;
    private int unitLen;

    public FileInput(String separator, int keyIndex, int unitLen) {
        super(separator, unitLen, null);
        this.separator = separator;
        this.keyIndex = keyIndex;
        this.unitLen = unitLen;
    }
}
