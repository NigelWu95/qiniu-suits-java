package com.qiniu.custom.miaop;

import com.qiniu.entry.InputInfoParser;
import com.qiniu.model.parameter.FileInputParams;
import com.qiniu.model.parameter.QhashParams;
import com.qiniu.service.datasource.FileInput;
import com.qiniu.service.interfaces.ILineProcess;

import java.util.Map;

public class MirrorKeyFileInput extends FileInput {

    public static void main(String[] args) throws Exception {

        FileInputParams fileInputParams = new FileInputParams("resources/.qiniu.properties");
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
        QhashParams qhashParams = new QhashParams("resources/.qiniu.properties");
        ILineProcess<Map<String, String>> processor = new MirrorSrcHash(qhashParams.getDomain(), resultFileDir);
        Map<String, String> infoIndexMap = new InputInfoParser().getInfoIndexMap(fileInputParams);
        MirrorKeyFileInput fileInput = new MirrorKeyFileInput(parseType, separator, infoIndexMap, 3, unitLen,
                resultFileDir);
        fileInput.process(maxThreads, sourceFilePath, processor);
        processor.closeResource();
    }

    public MirrorKeyFileInput(String parseType, String separator, Map<String, String> infoIndexMap, int retryCount, int unitLen,
                              String resultFileDir) {
        super(parseType, separator, infoIndexMap, retryCount, unitLen, resultFileDir);
    }
}
