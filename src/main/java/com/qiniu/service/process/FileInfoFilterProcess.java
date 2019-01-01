package com.qiniu.service.process;

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
    private ILineFilter<Map<String, String>> filter;
    private ILineProcess<Map<String, String>> nextProcessor;
    private String resultPath;
    private String resultFormat;
    private String resultSeparator;
    private List<String> rmFields;
    private String resultTag;
    private int resultIndex;
    private FileMap fileMap;
    private ITypeConvert<Map<String, String>, String> typeConverter;

    public FileInfoFilterProcess(FileFilter filter, String resultPath, String resultFormat, String resultSeparator,
                                 List<String> rmFields, int resultIndex) throws Exception {
        this.processName = "filter";
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
        this.resultPath = resultPath;
        this.resultFormat = resultFormat;
        this.resultSeparator = (resultSeparator == null || "".equals(resultSeparator)) ? "\t" : resultSeparator;
        this.rmFields = rmFields;
        this.resultTag = "";
        this.resultIndex = resultIndex;
        this.fileMap = new FileMap(resultPath, processName, String.valueOf(resultIndex));
        this.fileMap.initDefaultWriters();
        this.typeConverter = new InfoMapToString(resultFormat, resultSeparator, rmFields);
    }

    public FileInfoFilterProcess(FileFilter filter, String resultPath, String resultFormat, String resultSeparator,
                                 List<String> resultFields) throws Exception {
        this(filter, resultPath, resultFormat, resultSeparator, resultFields, 0);
    }

    public String getProcessName() {
        return this.processName;
    }

    public void setResultTag(String resultTag) {
        this.resultTag = resultTag == null ? "" : resultTag;
    }

    public FileInfoFilterProcess clone() throws CloneNotSupportedException {
        FileInfoFilterProcess fileInfoFilterProcess = (FileInfoFilterProcess)super.clone();
        fileInfoFilterProcess.fileMap = new FileMap(resultPath, processName, resultTag + String.valueOf(resultIndex++));
        try {
            fileInfoFilterProcess.fileMap.initDefaultWriters();
            fileInfoFilterProcess.typeConverter = new InfoMapToString(resultFormat, resultSeparator, rmFields);
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

    public void processLine(List<Map<String, String>> list) throws IOException {
        if (list == null || list.size() == 0) return;
        List<Map<String, String>> resultList = new ArrayList<>();
        List<String> writeList;
        try {
            for (Map<String, String> line : list) {
                if (filter.doFilter(line)) resultList.add(line);
            }
            writeList = typeConverter.convertToVList(resultList);
            if (writeList.size() > 0) fileMap.writeSuccess(String.join("\n", writeList));
            if (typeConverter.getErrorList().size() > 0)
                fileMap.writeError(String.join("\n", typeConverter.getErrorList()));
            if (nextProcessor != null) nextProcessor.processLine(resultList);
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
    }

    public void closeResource() {
        fileMap.closeWriter();
        if (nextProcessor != null) nextProcessor.closeResource();
    }
}
