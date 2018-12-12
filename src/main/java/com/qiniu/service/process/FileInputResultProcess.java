package com.qiniu.service.process;

import com.qiniu.common.QiniuException;
import com.qiniu.model.parameter.InfoMapParams;
import com.qiniu.persistence.FileMap;
import com.qiniu.service.interfaces.ILineProcess;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FileInputResultProcess implements ILineProcess<Map<String, String>>, Cloneable {

    private String processName;
    private InfoMapParams infoMapParams;
    private String resultFormat;
    private String separator;
    private String resultFileDir;
    private FileMap fileMap;
    private ILineProcess<Map<String, String>> nextProcessor;

    private void initBaseParams() {
        this.processName = "input";
    }

    public FileInputResultProcess(String resultFormat, String separator, String resultFileDir) {
        initBaseParams();
        this.resultFormat = (resultFormat == null || "".equals(resultFormat)) ? "json" : resultFormat;
        this.separator = (separator == null || "".equals(separator)) ? "\t" : separator;
        this.resultFileDir = resultFileDir;
        this.fileMap = new FileMap();
    }

    public FileInputResultProcess(String resultFormat, String separator, String resultFileDir, int resultFileIndex)
            throws IOException {
        this(resultFormat, separator, resultFileDir);
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

    public void processLine(List<Map<String, String>> infoMapList) throws QiniuException {
        if (infoMapList == null || infoMapList.size() == 0) return;
        try {
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
