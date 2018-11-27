package com.qiniu.custom.fantx;

import com.qiniu.model.parameter.AvinfoParams;
import com.qiniu.model.parameter.FileInputParams;
import com.qiniu.service.datasource.FileInput;
import com.qiniu.service.interfaces.ILineProcess;

public class AvinfoFileInput extends FileInput {

    public static void main(String[] args) throws Exception {

        FileInputParams fileInputParams = new FileInputParams("resources/.qiniu.properties");
        String filePath = fileInputParams.getFilePath();
        String separator = fileInputParams.getSeparator();
        int keyIndex = fileInputParams.getKeyIndex();
        int maxThreads = fileInputParams.getMaxThreads();
        int unitLen = fileInputParams.getUnitLen();
        String sourceFilePath = System.getProperty("user.dir") + System.getProperty("file.separator") + filePath;
        String resultFileDir = fileInputParams.getResultFileDir();
        AvinfoParams avinfoParams = new AvinfoParams("resources/.qiniu.properties");
        ILineProcess processor = new AvinfoProcess(avinfoParams.getDomain(), avinfoParams.getBucket(), resultFileDir);
        AvinfoFileInput fileInput = new AvinfoFileInput(separator, keyIndex, unitLen);
        fileInput.process(maxThreads, sourceFilePath, processor);
        processor.closeResource();
    }

    private String separator;
    private int keyIndex;
    private int unitLen;

    public AvinfoFileInput(String separator, int keyIndex, int unitLen) {
        super(separator, keyIndex, unitLen);
        this.separator = separator;
        this.keyIndex = keyIndex;
        this.unitLen = unitLen;
    }
}
