package com.qiniu.custom.miaop;

import com.qiniu.model.parameter.FileInputParams;
import com.qiniu.model.parameter.InfoMapParams;
import com.qiniu.model.parameter.QhashParams;
import com.qiniu.service.datasource.FileInput;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.process.FileInputResultProcess;

import java.util.Map;

public class MirrorKeyFileInput extends FileInput {

    public static void main(String[] args) throws Exception {

        FileInputParams fileInputParams = new FileInputParams("resources/.qiniu.properties");
        InfoMapParams infoMapParams = new InfoMapParams("resources/.qiniu.properties");
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
        QhashParams qhashParams = new QhashParams("resources/.qiniu.properties");
        ILineProcess<Map<String, String>> processor = new MirrorSrcHash(qhashParams.getDomain(), resultFileDir);
        MirrorKeyFileInput fileInput = new MirrorKeyFileInput(unitLen);
        ILineProcess<String> inputResultProcessor = new FileInputResultProcess(parserType, inputSeparator, infoMapParams,
                resultFormat, resultSeparator, resultFileDir, saveTotal);
        inputResultProcessor.setNextProcessor(processor);
        fileInput.process(maxThreads, sourceFilePath, inputResultProcessor);
        inputResultProcessor.closeResource();
    }

    public MirrorKeyFileInput(int unitLen) {
        super(unitLen);
    }
}
