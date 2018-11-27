package com.qiniu.custom.miaop;

import com.qiniu.model.parameter.QhashParams;
import com.qiniu.service.datasource.FileInput;
import com.qiniu.service.fileline.SplitLineParser;
import com.qiniu.service.interfaces.ILineParser;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.StringUtils;

import java.io.BufferedReader;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class MirrorKeyFileInput extends FileInput {

    public static void main(String[] args) throws Exception {

        String filePath = "../miaopai/keys";
        String separator = "\t";
        int keyIndex = 0;
        int maxThreads = 200;
        int unitLen = 3000;
        String sourceFilePath = System.getProperty("user.dir") + System.getProperty("file.separator") + filePath;
        String resultFileDir = "../miaopai/hash";
        QhashParams qhashParams = new QhashParams("resources/.qiniu.properties");
        ILineProcess processor = new MirrorSrcHash(qhashParams.getDomain(), resultFileDir);
        MirrorKeyFileInput fileInput = new MirrorKeyFileInput(separator, keyIndex, unitLen);
        fileInput.process(maxThreads, sourceFilePath, processor);
        processor.closeResource();
    }

    private String separator;
    private int keyIndex;
    private int unitLen;

    public MirrorKeyFileInput(String separator, int keyIndex, int unitLen) {
        super(separator, keyIndex, unitLen);
        this.separator = separator;
        this.keyIndex = keyIndex;
        this.unitLen = unitLen;
    }
}
