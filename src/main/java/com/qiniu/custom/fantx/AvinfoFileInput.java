package com.qiniu.custom.fantx;

import com.qiniu.model.parameter.AvinfoParams;
import com.qiniu.model.parameter.CommonParams;
import com.qiniu.model.parameter.FileInputParams;
import com.qiniu.service.datasource.FileInput;
import com.qiniu.service.fileline.SplitLineParser;
import com.qiniu.service.interfaces.ILineParser;
import com.qiniu.service.interfaces.IQossProcess;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.StringUtils;

import java.io.BufferedReader;
import java.util.List;
import java.util.stream.Collectors;

public class AvinfoFileInput extends FileInput {

    public static void run(boolean paramFromConfig, String[] args, String configFilePath) throws Exception {

        FileInputParams fileInputParams = paramFromConfig ?
                new FileInputParams(configFilePath) : new FileInputParams(args);
        String filePath = fileInputParams.getFilePath();
        String separator = fileInputParams.getSeparator();
        int keyIndex = fileInputParams.getKeyIndex();
        int maxThreads = fileInputParams.getMaxThreads();
        int unitLen = fileInputParams.getUnitLen();
        String sourceFilePath = System.getProperty("user.dir") + System.getProperty("file.separator") + filePath;
        CommonParams commonParams = paramFromConfig ? new CommonParams(configFilePath) : new CommonParams(args);
        String resultFileDir = commonParams.getResultFileDir();
        AvinfoParams avinfoParams = paramFromConfig ? new AvinfoParams(configFilePath) : new AvinfoParams(args);
        IQossProcess processor = new AvinfoProcess(avinfoParams.getDomain(), resultFileDir);
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

    @Override
    public void traverseByReader(int finalI, List<BufferedReader> sourceReaders, IQossProcess fileProcessor) {

        IQossProcess processor = null;
        ILineParser lineParser = new SplitLineParser(separator);
        try {
            BufferedReader bufferedReader = sourceReaders.get(finalI);
            if (fileProcessor != null) processor = fileProcessor.getNewInstance(finalI + 1);
            List<FileInfo> fileInfoList = bufferedReader.lines().parallel()
                    .map(line -> {
                        FileInfo fileInfo = new FileInfo();
                        fileInfo.key = lineParser.getItemList(line).get(keyIndex);
                        fileInfo.hash = lineParser.getItemList(line).get(keyIndex + 1);
                        return fileInfo;
                    })
                    .filter(fileInfo -> !StringUtils.isNullOrEmpty(fileInfo.key))
                    .collect(Collectors.toList());
            int size = fileInfoList.size()/unitLen + 1;
            for (int j = 0; j < size; j++) {
                List<FileInfo> processList = fileInfoList.subList(unitLen * j,
                        j == size - 1 ? fileInfoList.size() : unitLen * (j + 1));
                if (processor != null) processor.processFile(processList);
            }
            bufferedReader.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (processor != null) processor.closeResource();
        }
    }
}