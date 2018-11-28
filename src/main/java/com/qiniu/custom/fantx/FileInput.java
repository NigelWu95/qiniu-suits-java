package com.qiniu.custom.fantx;

import com.qiniu.model.parameter.AvinfoParams;
import com.qiniu.model.parameter.FileInputParams;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.media.QueryPfopResult;

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
        AvinfoParams avinfoParams = new AvinfoParams("resources/.qiniu-fantx.properties");
        // parse avinfo from files.
//        ILineProcess processor = new AvinfoProcess(avinfoParams.getDomain(), avinfoParams.getBucket(), resultFileDir);
        // query persistent id and parse
//        ILineProcess processor = new QueryPfopResult(resultFileDir);
        // filter pfop result
//        ILineProcess processor = new PfopResultProcess(resultFileDir);
        // process avinfo
        ILineProcess processor = new AvinfoProcess(avinfoParams.getBucket(), resultFileDir);
        FileInput fileInput = new FileInput(separator, keyIndex, unitLen);
        fileInput.process(maxThreads, sourceFilePath, processor);
        processor.closeResource();
    }

    private String separator;
    private int keyIndex;
    private int unitLen;

    public FileInput(String separator, int keyIndex, int unitLen) {
        super(separator, keyIndex, unitLen);
        this.separator = separator;
        this.keyIndex = keyIndex;
        this.unitLen = unitLen;
    }
}
