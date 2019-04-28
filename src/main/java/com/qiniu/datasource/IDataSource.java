package com.qiniu.datasource;

import com.qiniu.entry.CommonParams;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.persistence.IResultSave;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface IDataSource<T, S> {

    String getSourceName();

    void setSaveOptions(String savePath, boolean saveTotal, String format, String separator, List<String> rmFields);

    void setProcessor(ILineProcess<Map<String, String>> processor);

    void export(T source, S saver, ILineProcess<Map<String, String>> processor) throws IOException;

    void execInThread(T source, S saver, int order) throws Exception;

    void export() throws Exception;

    void setRetryTimes(int retryTimes);

    void updateSettings(CommonParams commonParams);
}
