package com.qiniu.service.process;

import com.qiniu.common.QiniuException;
import com.qiniu.model.parameter.InfoMapParams;
import com.qiniu.persistence.FileMap;
import com.qiniu.service.convert.FileLineToMap;
import com.qiniu.service.convert.InfoMapToString;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.interfaces.ITypeConvert;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FileInputResultProcess implements ILineProcess<String>, Cloneable {

    private String processName;
    private String parserType;
    private String inputSeparator;
    private InfoMapParams infoMapParams;
    private String resultFormat;
    private String separator;
    private String resultFileDir;
    private boolean saveTotal = false;
    private FileMap fileMap;
    private ITypeConvert<String, Map<String, String>> typeConverter;
    private ITypeConvert<Map<String, String>, String> writeTypeConverter;
    private ILineProcess<Map<String, String>> nextProcessor;

    private void initBaseParams() {
        this.processName = "input";
    }

    public FileInputResultProcess(String parserType, String inputSeparator, InfoMapParams infoMapParams,
                                  String resultFormat, String separator, String resultFileDir, boolean saveTotal) {
        initBaseParams();
        this.parserType = parserType;
        this.inputSeparator = (inputSeparator == null || "".equals(inputSeparator)) ? "\t" : inputSeparator;
        this.infoMapParams = infoMapParams;
        this.resultFormat = resultFormat;
        this.separator = separator;
        this.resultFileDir = resultFileDir;
        this.saveTotal = saveTotal;
        this.fileMap = new FileMap();
        this.typeConverter = new FileLineToMap(parserType, inputSeparator, infoMapParams);
        this.writeTypeConverter = new InfoMapToString(resultFormat, separator, true, true, true,
                true, true, true, true);
    }

    public FileInputResultProcess(String parserType, String inputSeparator, InfoMapParams infoMapParams,
                                  String resultFormat, String separator, String resultFileDir, boolean saveTotal,
                                  int resultFileIndex) throws IOException {
        this(parserType, inputSeparator, infoMapParams, resultFormat, separator, resultFileDir, saveTotal);
        fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public void setNextProcessor(ILineProcess<Map<String, String>> nextProcessor) {
        this.nextProcessor = nextProcessor;
    }

    public FileInputResultProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        FileInputResultProcess fileInputResultProcess = (FileInputResultProcess)super.clone();
        fileInputResultProcess.fileMap = new FileMap();
        try {
            fileInputResultProcess.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
            fileInputResultProcess.typeConverter = new FileLineToMap(parserType, inputSeparator, infoMapParams);
            fileInputResultProcess.writeTypeConverter = new InfoMapToString(resultFormat, separator, true,
                    true, true, true, true, true, true);
            if (nextProcessor != null) {
                fileInputResultProcess.nextProcessor = nextProcessor.getNewInstance(resultFileIndex);
            }
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return fileInputResultProcess;
    }

    public String getProcessName() {
        return processName;
    }

    public void processLine(List<String> fileLineList) throws QiniuException {
        if (fileLineList == null || fileLineList.size() == 0) return;
        List<Map<String, String>> infoMapList = typeConverter.convertToVList(fileLineList);
        try {
            if (saveTotal) {
                fileMap.writeSuccess(String.join("\n", writeTypeConverter.convertToVList(infoMapList)));
            }
            if (nextProcessor != null) nextProcessor.processLine(infoMapList);
        } catch (Exception e) {
            throw new QiniuException(e, e.getMessage());
        }
    }

    public void closeResource() {
        fileMap.closeWriter();
        if (nextProcessor != null) nextProcessor.closeResource();
    }
}
