package com.qiniu.custom.fantx;

import com.qiniu.model.parameter.AvinfoParams;
import com.qiniu.model.parameter.FileInputParams;
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

public class AvinfoFileInput extends FileInput {

    public static void main(String[] args) throws Exception {

        FileInputParams fileInputParams = new FileInputParams("resources/.qiniu.properties");
        String filePath = fileInputParams.getFilePath();
        filePath = "../../fantexi/avinfo";
        String separator = fileInputParams.getSeparator();
        int keyIndex = fileInputParams.getKeyIndex();
        int maxThreads = fileInputParams.getMaxThreads();
        int unitLen = fileInputParams.getUnitLen();
        String sourceFilePath = System.getProperty("user.dir") + System.getProperty("file.separator") + filePath;
        String resultFileDir = fileInputParams.getResultFileDir();
        resultFileDir = "../../fantexi/avthumb";
        AvinfoParams avinfoParams = new AvinfoParams("resources/.qiniu.properties");
        ILineProcess processor = new AvinfoProcess(avinfoParams.getDomain(), resultFileDir);
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
    public void traverseByReader(int finalI, List<BufferedReader> sourceReaders, ILineProcess fileProcessor) {

        ILineProcess processor = null;
        ILineParser lineParser = new SplitLineParser(separator);
        try {
            BufferedReader bufferedReader = sourceReaders.get(finalI);
            if (fileProcessor != null) processor = fileProcessor.getNewInstance(finalI + 1);
            List<Map<String, String>> lineList = bufferedReader.lines().parallel()
                    .map(lineParser::getItemMap)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            int size = lineList.size()/unitLen + 1;
            for (int j = 0; j < size; j++) {
                List<Map<String, String>> processList = lineList.subList(unitLen * j,
                        j == size - 1 ? lineList.size() : unitLen * (j + 1));
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