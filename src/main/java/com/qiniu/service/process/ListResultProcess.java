package com.qiniu.service.process;

import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.service.convert.FileInfoToMap;
import com.qiniu.service.convert.FileInfoToString;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.storage.model.FileInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ListResultProcess implements ILineProcess<FileInfo>, Cloneable {

    private String processName;
    private int retryCount = 3;
    private String resultFormat;
    private String separator;
    private String resultFileDir;
    private FileMap fileMap;
    private ITypeConvert<FileInfo, String> typeConverter;
    private boolean saveTotal = false;
    private ILineProcess<Map<String, String>> nextProcessor;
    private ITypeConvert<FileInfo, Map<String, String>> nextTypeConverter;

    private void initBaseParams() {
        this.processName = "list";
    }

    public ListResultProcess(String resultFormat, String separator, String resultFileDir, boolean saveTotal) {
        initBaseParams();
        this.resultFormat = resultFormat;
        this.separator = (separator == null || "".equals(separator)) ? "\t" : separator;
        this.resultFileDir = resultFileDir;
        this.saveTotal = saveTotal;
        this.fileMap = new FileMap();
        this.typeConverter = new FileInfoToString(resultFormat, separator);
    }

    public ListResultProcess(String resultFormat, String separator, String resultFileDir, int resultFileIndex,
                             boolean saveTotal) throws IOException {
        this(resultFormat, separator, resultFileDir, saveTotal);
        fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public void setNextProcessor(ILineProcess<Map<String, String>> nextProcessor) {
        this.nextProcessor = nextProcessor;
        this.nextTypeConverter = new FileInfoToMap();
    }

    public ListResultProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        ListResultProcess listResultProcess = (ListResultProcess)super.clone();
        listResultProcess.fileMap = new FileMap();
        try {
            listResultProcess.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
            listResultProcess.typeConverter = new FileInfoToString(resultFormat, separator);
            if (nextProcessor != null) {
                listResultProcess.nextProcessor = nextProcessor.getNewInstance(resultFileIndex);
                listResultProcess.nextTypeConverter = new FileInfoToMap();
            }
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return listResultProcess;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public String getProcessName() {
        return processName;
    }

    public void processLine(List<FileInfo> fileInfoList) throws QiniuException {
        if (fileInfoList == null || fileInfoList.size() == 0) return;

        try {
            if (saveTotal) {
                fileMap.writeKeyFile("total", String.join("\n", typeConverter.convertToVList(fileInfoList)));
            }
            fileMap.writeSuccess(String.join("\n", typeConverter.convertToVList(fileInfoList)));
            if (nextProcessor != null) nextProcessor.processLine(nextTypeConverter.convertToVList(fileInfoList));
        } catch (Exception e) {
            throw new QiniuException(e, e.getMessage());
        }
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}
