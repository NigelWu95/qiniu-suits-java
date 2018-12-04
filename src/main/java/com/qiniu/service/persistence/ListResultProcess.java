package com.qiniu.service.persistence;

import com.qiniu.common.FileMap;
import com.qiniu.common.ListFileAntiFilter;
import com.qiniu.common.ListFileFilter;
import com.qiniu.common.QiniuException;
import com.qiniu.service.convert.FileInfoToMap;
import com.qiniu.service.convert.FileInfoToString;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.ListFileFilterUtils;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class ListResultProcess implements ILineProcess<FileInfo>, Cloneable {

    private String processName;
    private int retryCount = 3;
    private String resultFormat;
    private String separator;
    private String resultFileDir;
    private FileMap fileMap;
    private ITypeConvert typeConverter;
    private ListFileFilter filter;
    private ListFileAntiFilter antiFilter;
    private boolean doFilter;
    private boolean doAntiFilter;
    private boolean saveTotal = false;
    private ILineProcess<FileInfo> nextProcessor;
    private ITypeConvert nextTypeConverter;

    private void initBaseParams() {
        this.processName = "list";
    }

    public ListResultProcess(String resultFormat, String separator, String resultFileDir) {
        initBaseParams();
        this.resultFormat = resultFormat;
        this.separator = (separator == null || "".equals(separator)) ? "\t" : separator;
        this.resultFileDir = resultFileDir;
        this.fileMap = new FileMap();
        this.typeConverter = new FileInfoToString(resultFormat, "\t");
    }

    public ListResultProcess(String resultFormat, String separator, String resultFileDir, int resultFileIndex)
            throws IOException {
        this(resultFormat, separator, resultFileDir);
        fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public void setNextProcessor(ILineProcess<FileInfo> nextProcessor) {
        this.nextProcessor = nextProcessor;
        this.nextTypeConverter = new FileInfoToMap();
    }

    public ListResultProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        ListResultProcess listResultProcess = (ListResultProcess)super.clone();
        listResultProcess.fileMap = new FileMap();
        try {
            listResultProcess.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
            if (nextProcessor != null) listResultProcess.nextProcessor = nextProcessor.getNewInstance(resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return listResultProcess;
    }

    public void setFilter(ListFileFilter listFileFilter, ListFileAntiFilter listFileAntiFilter) {
        this.filter = listFileFilter;
        this.antiFilter = listFileAntiFilter;
        this.doFilter = ListFileFilterUtils.checkListFileFilter(listFileFilter);
        this.doAntiFilter = ListFileFilterUtils.checkListFileAntiFilter(listFileAntiFilter);
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
            if (doFilter || doAntiFilter) {
                if (saveTotal) {
                    fileMap.writeOther(String.join("\n", typeConverter.convertToVList(fileInfoList)));
                }
                if (doFilter) {
                    fileInfoList = fileInfoList.parallelStream()
                            .filter(fileInfo -> filter.doFileFilter(fileInfo))
                            .collect(Collectors.toList());
                } else if (doAntiFilter) {
                    fileInfoList = fileInfoList.parallelStream()
                            .filter(fileInfo -> antiFilter.doFileAntiFilter(fileInfo))
                            .collect(Collectors.toList());
                } else {
                    fileInfoList = fileInfoList.parallelStream()
                            .filter(fileInfo -> filter.doFileFilter(fileInfo) && antiFilter.doFileAntiFilter(fileInfo))
                            .collect(Collectors.toList());
                }
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
