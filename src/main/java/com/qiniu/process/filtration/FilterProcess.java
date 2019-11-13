package com.qiniu.process.filtration;

import com.qiniu.interfaces.ILineFilter;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class FilterProcess<T> implements ILineProcess<T>, Cloneable {

    private String processName;
    private ILineFilter<T> filter;
    private boolean strictError; // 严格错误模式，如果不匹配就抛出异常或记录错误
    private ILineProcess<T> nextProcessor;
    private String savePath;
    private AtomicInteger saveIndex;
    private FileSaveMapper fileSaveMapper;
    private ITypeConvert<T, String> typeConverter;
    private boolean canceled;

    public FilterProcess(BaseFilter<T> baseFilter, SeniorFilter<T> seniorFilter) throws Exception {
        this.processName = "filter";
        this.filter = newFilter(baseFilter, seniorFilter);
    }

    public FilterProcess(BaseFilter<T> baseFilter, SeniorFilter<T> seniorFilter, String savePath, int saveIndex) throws Exception {
        this(baseFilter, seniorFilter);
        this.savePath = savePath;
        this.saveIndex = new AtomicInteger(saveIndex);
        this.fileSaveMapper = new FileSaveMapper(savePath, processName, String.valueOf(saveIndex));
        this.fileSaveMapper.preAddWriter("not_match");
        this.typeConverter = newPersistConverter();
    }

    public FilterProcess(BaseFilter<T> filter, SeniorFilter<T> checker, String savePath) throws Exception {
        this(filter, checker, savePath, 0);
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

    protected ITypeConvert<T, String> newPersistConverter() throws IOException {
        throw new IOException("if you need persistence, please override this method to provide a converter.");
    }

    public String getProcessName() {
        return this.processName;
    }

    public boolean isStrictError() {
        return strictError;
    }

    public void setStrictError(boolean strictError) {
        this.strictError = strictError;
    }

    public void setNextProcessor(ILineProcess<T> nextProcessor) {
        this.nextProcessor = nextProcessor;
        if (nextProcessor != null) processName = nextProcessor.getProcessName() + "_after_" + processName;
    }

    public ILineProcess<T> getNextProcessor() {
        return nextProcessor;
    }

    @SuppressWarnings("unchecked")
    public FilterProcess<T> clone() throws CloneNotSupportedException {
        FilterProcess<T> mapFilter = (FilterProcess<T>)super.clone();
        if (nextProcessor != null) mapFilter.nextProcessor = nextProcessor.clone();
        if (fileSaveMapper == null) return mapFilter;
        try {
            mapFilter.fileSaveMapper = new FileSaveMapper(savePath, processName, String.valueOf(saveIndex.addAndGet(1)));
            mapFilter.fileSaveMapper.preAddWriter("not_match");
            mapFilter.typeConverter = newPersistConverter();
        } catch (IOException e) {
            throw new CloneNotSupportedException(e.getMessage() + ", init writer failed.");
        }
        return mapFilter;
    }

    public String processLine(T line) throws IOException {
        try {
            if (filter.doFilter(line)) {
                if (nextProcessor == null) return String.valueOf(line);
                else return nextProcessor.processLine(line);
            } else if (strictError) {
                throw new IOException("not match");
            } else {
                return "false";
            }
        } catch (NullPointerException e) {
            if (canceled) {
                throw new IOException("processor is canceled state.", e);
            } else if (filter == null) { // 如果是关闭了那么 filter 应该为 null
                throw new IOException("input is empty or the processor may be already closed.", e);
            } else {
                throw new IOException("instance without savePath can not call this batch process method.", e);
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    public void processLine(List<T> list) throws IOException {
        if (list == null || list.size() == 0) return;
        List<T> filterList = new ArrayList<>();
        for (T line : list) {
            try {
                if (filter.doFilter(line)) filterList.add(line);
                else fileSaveMapper.writeToKey("not_match", typeConverter.convertToV(line), false);
            } catch (NullPointerException e) {
                if (canceled) {
//                // nothing to do
                } else if (filter == null) { // 如果是关闭了那么 filter 应该为 null
                    throw new IOException("input is empty or the processor may be already closed.", e);
                } else {
                    throw new IOException("instance without savePath can not call this batch process method.", e);
                }
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e.getMessage(), e);
            }
        }
        // 默认在不进行进一步处理的情况下直接保存结果，如果需要进一步处理则不保存过滤的结果。
        if (nextProcessor == null) {
            List<String> writeList = typeConverter.convertToVList(filterList);
            if (writeList.size() > 0) fileSaveMapper.writeSuccess(String.join("\n", writeList), false);
            if (typeConverter.errorSize() > 0) fileSaveMapper.writeError(typeConverter.errorLines(), false);
        } else {
            nextProcessor.processLine(filterList);
        }
    }

    public void closeResource() {
        filter = null;
        if (nextProcessor != null) nextProcessor.closeResource();
        savePath = null;
        if (fileSaveMapper != null) fileSaveMapper.closeWriters();
        fileSaveMapper = null;
        typeConverter = null;
    }

    public void cancel() {
        canceled = true;
        closeResource();
    }
}
