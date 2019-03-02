package com.qiniu.service.filtration;

import com.qiniu.common.QiniuException;
import com.qiniu.persistence.FileMap;
import com.qiniu.service.convert.MapToString;
import com.qiniu.service.interfaces.ILineFilter;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.interfaces.ITypeConvert;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FilterProcess implements ILineProcess<Map<String, String>>, Cloneable {

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

    public FilterProcess(BaseFieldsFilter filter, String checkType, String resultPath, String resultFormat,
                         String resultSeparator, List<String> rmFields, int resultIndex) throws Exception {
        this.processName = "filter";
        SeniorChecker seniorChecker = new SeniorChecker();
        Method checkMethod = (checkType == null || "".equals(checkType)) ? null :
                SeniorChecker.class.getMethod("checkMimeType", Map.class);
        List<Method> fileTerMethods = new ArrayList<Method>() {{
            if (filter.checkKeyPrefix()) add(filter.getClass().getMethod("filterKeyPrefix", Map.class));
            if (filter.checkKeySuffix()) add(filter.getClass().getMethod("filterKeySuffix", Map.class));
            if (filter.checkKeyInner()) add(filter.getClass().getMethod("filterKeyInner", Map.class));
            if (filter.checkKeyRegex()) add(filter.getClass().getMethod("filterKeyRegex", Map.class));
            if (filter.checkPutTime()) add(filter.getClass().getMethod("filterPutTime", Map.class));
            if (filter.checkMime()) add(filter.getClass().getMethod("filterMimeType", Map.class));
            if (filter.checkType()) add(filter.getClass().getMethod("filterType", Map.class));
            if (filter.checkStatus()) add(filter.getClass().getMethod("filterStatus", Map.class));
            if (filter.checkAntiKeyPrefix()) add(filter.getClass().getMethod("filterAntiKeyPrefix", Map.class));
            if (filter.checkAntiKeySuffix()) add(filter.getClass().getMethod("filterAntiKeySuffix", Map.class));
            if (filter.checkAntiKeyInner()) add(filter.getClass().getMethod("filterAntiKeyInner", Map.class));
            if (filter.checkAntiKeyRegex()) add(filter.getClass().getMethod("filterAntiKeyRegex", Map.class));
            if (filter.checkAntiMime()) add(filter.getClass().getMethod("filterAntiMimeType", Map.class));
        }};
        this.filter = line -> {
            boolean result = true;
            for (Method method : fileTerMethods) {
                result = result && (boolean) method.invoke(filter, line);
            }
            return result
//                    && (boolean) checkMethod.invoke(seniorChecker, line)
                    ;
        };
        this.resultPath = resultPath;
        this.resultFormat = resultFormat;
        this.resultSeparator = (resultSeparator == null || "".equals(resultSeparator)) ? "\t" : resultSeparator;
        this.rmFields = rmFields;
        this.resultTag = "";
        this.resultIndex = resultIndex;
        this.fileMap = new FileMap(resultPath, processName, String.valueOf(resultIndex));
        this.fileMap.initDefaultWriters();
        this.typeConverter = new MapToString(resultFormat, resultSeparator, rmFields);
    }

    public FilterProcess(BaseFieldsFilter filter, String checkType, String resultPath, String resultFormat,
                         String resultSeparator, List<String> removeFields) throws Exception {
        this(filter, checkType, resultPath, resultFormat, resultSeparator, removeFields, 0);
    }

    public String getProcessName() {
        return this.processName;
    }

    public void setResultTag(String resultTag) {
        this.resultTag = resultTag == null ? "" : resultTag;
    }

    public FilterProcess clone() throws CloneNotSupportedException {
        FilterProcess filterProcess = (FilterProcess)super.clone();
        filterProcess.fileMap = new FileMap(resultPath, processName, resultTag + String.valueOf(++resultIndex));
        try {
            filterProcess.fileMap.initDefaultWriters();
            filterProcess.typeConverter = new MapToString(resultFormat, resultSeparator, rmFields);
            if (nextProcessor != null) {
                filterProcess.nextProcessor = nextProcessor.clone();
            }
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return filterProcess;
    }

    public void setNextProcessor(ILineProcess<Map<String, String>> nextProcessor) {
        this.nextProcessor = nextProcessor;
    }

    public void processLine(List<Map<String, String>> list) throws IOException {
        if (list == null || list.size() == 0) return;
        List<Map<String, String>> resultList = new ArrayList<>();
        List<String> writeList;
        for (Map<String, String> line : list) {
            try {
                if (filter.doFilter(line)) resultList.add(line);
            } catch (Exception e) {
                throw new QiniuException(e);
            }
        }
        writeList = typeConverter.convertToVList(resultList);
        if (writeList.size() > 0) fileMap.writeSuccess(String.join("\n", writeList), false);
        if (typeConverter.getErrorList().size() > 0)
            fileMap.writeError(String.join("\n", typeConverter.getErrorList()), false);
        if (nextProcessor != null) nextProcessor.processLine(resultList);
    }

    public void closeResource() {
        fileMap.closeWriters();
        if (nextProcessor != null) nextProcessor.closeResource();
    }
}
