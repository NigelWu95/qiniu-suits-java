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
    private String resultFileDir;
    private String resultFormat;
    private String separator;
    private FileMap fileMap;
    protected int retryCount = 3;
    private ITypeConvert<Map<String, String>, String> typeConverter;
    private ILineFilter<Map<String, String>> filter;
    private ILineProcess<Map<String, String>> nextProcessor;

    private void initBaseParams() {
        this.processName = "filter";
    }

    public FileInfoFilterProcess(String resultFileDir, String resultFormat, String separator, FileFilter filter)
            throws Exception {
        initBaseParams();
        this.resultFormat = resultFormat;
        this.separator = (separator == null || "".equals(separator)) ? "\t" : separator;
        this.resultFileDir = resultFileDir;
        this.fileMap = new FileMap();
        this.typeConverter = new InfoMapToString(resultFormat, separator, true, true, true,
                true, true, true, true);
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

    public FileInfoFilterProcess(String resultFormat, String separator, String resultFileDir, int resultFileIndex,
                                 FileFilter filter) throws Exception {
        this(resultFormat, separator, resultFileDir, filter);
        fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public FileInfoFilterProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        FileInfoFilterProcess fileInfoFilterProcess = (FileInfoFilterProcess)super.clone();
        fileInfoFilterProcess.fileMap = new FileMap();
        try {
            fileInfoFilterProcess.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
            fileInfoFilterProcess.typeConverter = new InfoMapToString(resultFormat, separator, true, true,
                    true, true, true, true, true);
            if (nextProcessor != null) {
                fileInfoFilterProcess.nextProcessor = nextProcessor.getNewInstance(resultFileIndex);
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

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public void processLine(List<Map<String, String>> list) throws QiniuException {
        if (list == null || list.size() == 0) return;
        List<Map<String, String>> resultList = new ArrayList<>();
        try {
            for (Map<String, String> line : list) {
                boolean result = true;
                try {
                    result = filter.doFilter(line);
                } catch (ReflectiveOperationException e) {
                    while (retryCount > 0) {
                        try {
                            result = filter.doFilter(line);
                            retryCount = 0;
                        } catch (ReflectiveOperationException e1) {
                            retryCount--;
                            if (retryCount <= 0) throw e1;
                        }
                    }
                }
                if (result) resultList.add(line);
            }
            fileMap.writeSuccess(String.join("\n", typeConverter.convertToVList(resultList)));
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
