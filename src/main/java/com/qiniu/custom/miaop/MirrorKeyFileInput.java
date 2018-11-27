package com.qiniu.custom.miaop;

import com.qiniu.service.datasource.FileInput;
import com.qiniu.service.fileline.SplitLineParser;
import com.qiniu.service.interfaces.ILineParser;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.StringUtils;

import java.io.BufferedReader;
import java.util.List;
import java.util.stream.Collectors;

public class MirrorKeyFileInput extends FileInput {

    public static void main(String[] args) {

        String filePath = "../miaopai/keys";
        String separator = "\t";
        int keyIndex = 0;
        int maxThreads = 200;
        int unitLen = 3000;
        String sourceFilePath = System.getProperty("user.dir") + System.getProperty("file.separator") + filePath;
        String resultFileDir = "../miaopai/hash";
        ILineProcess processor = new MirrorSrcHash("miaopai-s.oss-cn-beijing.aliyuncs.com", resultFileDir);
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

    @Override
    public void traverseByReader(int finalI, List<BufferedReader> sourceReaders, ILineProcess fileProcessor) {

        ILineProcess processor = null;
        ILineParser lineParser = new SplitLineParser(separator);
        try {
            BufferedReader bufferedReader = sourceReaders.get(finalI);
            if (fileProcessor != null) processor = fileProcessor.getNewInstance(finalI + 1);
            List<FileInfo> fileInfoList = bufferedReader.lines().parallel()
                    .map(line -> {
                        FileInfo fileInfo = new FileInfo();
                        fileInfo.key = lineParser.getItemList(line).get(keyIndex);
                        return fileInfo;
                    })
                    .filter(fileInfo -> !StringUtils.isNullOrEmpty(fileInfo.key))
                    .collect(Collectors.toList());
            int size = fileInfoList.size()/unitLen + 1;
            for (int j = 0; j < size; j++) {
                List<FileInfo> processList = fileInfoList.subList(unitLen * j,
                        j == size - 1 ? fileInfoList.size() : unitLen * (j + 1));
                if (processor != null) processor.processLine(processList);
            }
            bufferedReader.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (processor != null) processor.closeResource();
        }
    }
}