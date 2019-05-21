package com.qiniu.process.filtration;

import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.ILineFilter;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public abstract class FilterProcess<T> implements ILineProcess<T>, Cloneable {

    protected String processName;
    protected ILineFilter<T> filter;
    protected ILineProcess<T> nextProcessor;
    protected String savePath;
    protected String saveFormat;
    protected String saveSeparator;
    protected List<String> rmFields;
    protected int saveIndex;
    protected FileSaveMapper fileSaveMapper;
    protected ITypeConvert<T, String> typeConverter;

    public FilterProcess(BaseFilter<T> baseFilter, SeniorFilter<T> seniorFilter, String savePath,
                         String saveFormat, String saveSeparator, List<String> rmFields, int saveIndex)
            throws Exception {
        this.processName = "filter";
        this.filter = newFilter(baseFilter, seniorFilter);
        this.savePath = savePath;
        this.saveFormat = saveFormat;
        this.saveSeparator = saveSeparator;
        this.rmFields = rmFields;
        this.saveIndex = saveIndex;
        this.fileSaveMapper = new FileSaveMapper(savePath, processName, String.valueOf(saveIndex));
        this.typeConverter = newTypeConverter();
    }

    public FilterProcess(BaseFilter<T> filter, SeniorFilter<T> checker, String savePath, String saveFormat,
                         String saveSeparator, List<String> rmFields) throws Exception {
        this(filter, checker, savePath, saveFormat, saveSeparator, rmFields, 0);
    }

    public ILineFilter<T> newFilter(BaseFilter<T> baseFilter, SeniorFilter<T> seniorFilter) throws NoSuchMethodException {
        List<Method> filterMethods = new ArrayList<Method>() {{
            if (baseFilter != null) {
                Class<?> clazz = baseFilter.getClass();
                if (baseFilter.checkKeyCon()) add(clazz.getMethod("filterKey", Object.class));
                if (baseFilter.checkMimeTypeCon()) add(clazz.getMethod("filterMimeType", Object.class));
                if (baseFilter.checkDatetimeCon()) add(clazz.getMethod("filterDatetime", Object.class));
                if (baseFilter.checkTypeCon()) add(clazz.getMethod("filterType", Object.class));
                if (baseFilter.checkStatusCon()) add(clazz.getMethod("filterStatus", Object.class));
            }
        }};
        List<Method> checkMethods = new ArrayList<Method>() {{
            if (seniorFilter != null) {
                Class<?> clazz = seniorFilter.getClass();
                if (seniorFilter.checkExtMime()) add(clazz.getMethod("checkMimeType", Object.class));
            }
        }};
        return line -> {
            boolean result;
            for (Method method : filterMethods) {
                result = (boolean) method.invoke(baseFilter, line);
                if (!result) return false;
            }
            for (Method method : checkMethods) {
                result = (boolean) method.invoke(seniorFilter, line);
                if (!result) return false;
            }
            return true;
        };
    }

    protected abstract ITypeConvert<T, String> newTypeConverter() throws IOException;

    public String getProcessName() {
        return this.processName;
    }

    public void setNextProcessor(ILineProcess<T> nextProcessor) {
        this.nextProcessor = nextProcessor;
    }

    public ILineProcess<T> getNextProcessor() {
        return nextProcessor;
    }

    @SuppressWarnings("unchecked")
    public FilterProcess<T> clone() throws CloneNotSupportedException {
        FilterProcess<T> mapFilter = (FilterProcess<T>)super.clone();
        try {
            mapFilter.fileSaveMapper = new FileSaveMapper(savePath, processName, String.valueOf(++saveIndex));
            mapFilter.typeConverter = newTypeConverter();
            if (nextProcessor != null) {
                mapFilter.nextProcessor = nextProcessor.clone();
            }
        } catch (IOException e) {
            throw new CloneNotSupportedException(e.getMessage() + ", init writer failed.");
        }
        return mapFilter;
    }

    public String processLine(T line) throws IOException {
        try {
            if (filter.doFilter(line)) return String.valueOf(true);
            else return  "false";
        } catch (Exception e) {
            throw new QiniuException(e, e.getMessage());
        }
    }

    public void processLine(List<T> list) throws IOException {
        if (list == null || list.size() == 0) return;
        List<T> filterList = new ArrayList<>();
        for (T line : list) {
            try {
                if (filter.doFilter(line)) filterList.add(line);
            } catch (Exception e) {
                throw new QiniuException(e, e.getMessage());
            }
        }
        // 默认在不进行进一步处理的情况下直接保存结果，如果需要进一步处理则不保存过滤的结果。
        if (nextProcessor == null) {
            List<String> writeList = typeConverter.convertToVList(filterList);
            if (writeList.size() > 0) fileSaveMapper.writeSuccess(String.join("\n", writeList), false);
            if (typeConverter.errorSize() > 0)
                fileSaveMapper.writeError(String.join("\n", typeConverter.consumeErrors()), false);
        } else {
            nextProcessor.processLine(filterList);
        }
    }

    public void closeResource() {
        fileSaveMapper.closeWriters();
        if (nextProcessor != null) nextProcessor.closeResource();
    }
}
