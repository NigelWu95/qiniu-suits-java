package com.qiniu.service.process;

import com.qiniu.common.QiniuException;
import com.qiniu.service.interfaces.ILineFilter;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.storage.model.FileInfo;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FilterProcess implements ILineProcess<FileInfo>, Cloneable {

    private String processName;
    private ITypeConvert<FileInfo, Map<String, String>> typeConverter;
    private ILineFilter<Map<String, String>> filter;
    private boolean saveTotal = false;
    private ILineProcess<Map<String, String>> nextProcessor;
    private ITypeConvert<FileInfo, Map<String, String>> nextTypeConverter;

    private void initBaseParams() {
        this.processName = "filter";
    }

    public FilterProcess(FileFilter filter) throws NoSuchMethodException {
        List<String> methodNameList = new ArrayList<String>(){{
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
        List<Method> methods = new ArrayList<Method>(){{
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

    public String getProcessName() {
        return processName;
    }

    public ILineProcess<FileInfo> getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        return null;
    }

    public void setRetryCount(int retryCount) {

    }

    public void processLine(List<FileInfo> fileInfoList) throws QiniuException {
        if (fileInfoList == null || fileInfoList.size() == 0) return;

        nextProcessor.processLine(typeConverter.convertToVList(fileInfoList).parallelStream()
                .filter(line -> {
                    try {
                        return filter.doFilter(line);
                    } catch (ReflectiveOperationException e) {
                        // TODO
                        e.printStackTrace();
                        return true;
                    }
                })
                .collect(Collectors.toList()));
    }

    public void closeResource() {

    }
}
