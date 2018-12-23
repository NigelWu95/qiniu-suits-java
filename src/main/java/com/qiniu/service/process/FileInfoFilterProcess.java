package com.qiniu.service.process;

import com.qiniu.common.QiniuException;
import com.qiniu.persistence.FileMap;
import com.qiniu.service.convert.InfoMapToString;
import com.qiniu.service.interfaces.ILineFilter;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.interfaces.ITypeConvert;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FileInfoFilterProcess implements ILineProcess<Map<String, String>>, Cloneable {

    private String processName;
    private String resultPath;
    private String resultFormat;
    private String separator;
    private int resultIndex;
    private FileMap fileMap;
    private ITypeConvert<Map<String, String>, String> typeConverter;
    private ILineFilter<Map<String, String>> filter;
    private List<String> usedFields;
    private ILineProcess<Map<String, String>> nextProcessor;

    public FileInfoFilterProcess(String resultFormat, String separator, String resultPath, int resultIndex,
                                 FileFilter filter, List<String> usedFields) throws Exception {
        this.processName = "filter";
        this.resultFormat = resultFormat;
        this.separator = (separator == null || "".equals(separator)) ? "\t" : separator;
        this.resultPath = resultPath;
        this.resultIndex = resultIndex;
        this.fileMap = new FileMap();
        this.fileMap.initWriter(resultPath, processName, resultIndex);
        this.typeConverter = new InfoMapToString(resultFormat, separator, usedFields);
        this.usedFields = usedFields;
        List<String> methodNameList = new ArrayList<String>() {{
            if (filter.checkKeyPrefix()) add("filterKeyPrefix");
            if (filter.checkKeySuffix()) add("filterKeySuffix");
            if (filter.checkKeyRegex()) add("filterKeyRegex");
            if (filter.checkPutTime()) add("filterPutTime");
            if (filter.checkMime()) add("filterMime");
            if (filter.checkType()) add("filterType");
            if (filter.checkAntiKeyPrefix()) add("filterAntiKeyPrefix");
            if (filter.checkAntiKeySuffix()) add("filterAntiKeySuffix");
            if (filter.checkAntiKeyRegex()) add("filterAntiKeyRegex");
            if (filter.checkAntiMime()) add("filterAntiMime");
        }};
        List<Method> methods = new ArrayList<Method>() {{
            for (String name : methodNameList) {
                add(filter.getClass().getMethod(name, Map.class));
            }
        }};
        this.filter = line -> {
            boolean result = true;
            for (Method method : methods) {
                result = result && (boolean) method.invoke(filter, line);
            }
            return result;
        };
    }

    public FileInfoFilterProcess(String resultFormat, String separator, String resultPath, FileFilter filter,
                                 List<String> usedFields) throws Exception {
        this(resultFormat, separator, resultPath, 0, filter, usedFields);
    }

    public FileInfoFilterProcess clone() throws CloneNotSupportedException {
        FileInfoFilterProcess fileInfoFilterProcess = (FileInfoFilterProcess)super.clone();
        fileInfoFilterProcess.fileMap = new FileMap();
        try {
            fileInfoFilterProcess.fileMap.initWriter(resultPath, processName, resultIndex++);
            fileInfoFilterProcess.typeConverter = new InfoMapToString(resultFormat, separator, usedFields);
            if (nextProcessor != null) {
                fileInfoFilterProcess.nextProcessor = nextProcessor.clone();
            }
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return fileInfoFilterProcess;
    }

    public void setNextProcessor(ILineProcess<Map<String, String>> nextProcessor) {
        this.nextProcessor = nextProcessor;
    }

    public String getProcessName() {
        return processName;
    }

    public void processLine(List<Map<String, String>> list) throws QiniuException {
        if (list == null || list.size() == 0) return;
        List<Map<String, String>> resultList = new ArrayList<>();
        try {
            for (Map<String, String> line : list) {
                if (filter.doFilter(line)) resultList.add(line);
            }
            fileMap.writeSuccess(String.join("\n", typeConverter.convertToVList(resultList)));
            if (typeConverter.getErrorList().size() > 0)
                fileMap.writeErrorOrNull(String.join("\n", typeConverter.getErrorList()));
            if (nextProcessor != null) nextProcessor.processLine(resultList);
        } catch (Exception e) {
            throw new QiniuException(e, e.getMessage());
        }
    }

    public void closeResource() {
        fileMap.closeWriter();
        if (nextProcessor != null) nextProcessor.closeResource();
    }
}
