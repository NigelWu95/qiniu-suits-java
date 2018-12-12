package com.qiniu.service.process;

import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.service.convert.FileInfoToMap;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.storage.model.FileInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ListResultProcess implements ILineProcess<FileInfo>, Cloneable {

    private String processName;
    private String resultFileDir;
    private FileMap fileMap;
    private ITypeConvert<FileInfo, Map<String, String>> nextTypeConverter;
    private ILineProcess<Map<String, String>> nextProcessor;

    private void initBaseParams() {
        this.processName = "list";
    }

    public ListResultProcess(String resultFileDir) {
        initBaseParams();
        this.resultFileDir = resultFileDir;
        this.fileMap = new FileMap();
    }

    public ListResultProcess(String resultFileDir, int resultFileIndex) throws IOException {
        this(resultFileDir);
        fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public void setNextProcessor(ILineProcess<Map<String, String>> nextProcessor) {
        this.nextTypeConverter = new FileInfoToMap(true, true, true, true, true,
                true, true);
        this.nextProcessor = nextProcessor;
    }

    public ListResultProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        ListResultProcess listResultProcess = (ListResultProcess)super.clone();
        listResultProcess.fileMap = new FileMap();
        try {
            listResultProcess.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
            if (nextProcessor != null) {
                listResultProcess.nextTypeConverter = new FileInfoToMap(true, true, true,
                        true, true, true, true);
                listResultProcess.nextProcessor = nextProcessor.getNewInstance(resultFileIndex);
            }
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return listResultProcess;
    }

    public String getProcessName() {
        return processName;
    }

    public void processLine(List<FileInfo> fileInfoList) throws QiniuException {
        if (fileInfoList == null || fileInfoList.size() == 0) return;
        try {
            if (nextProcessor != null) nextProcessor.processLine(nextTypeConverter.convertToVList(fileInfoList));
        } catch (Exception e) {
            throw new QiniuException(e, e.getMessage());
        }
    }

    public void closeResource() {
        fileMap.closeWriter();
        if (nextProcessor != null) nextProcessor.closeResource();
    }
}
