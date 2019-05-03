package com.qiniu.datasource;

import com.qiniu.entry.CommonParams;
import com.qiniu.interfaces.ILineProcess;

import java.io.IOException;
import java.util.List;

public interface IDataSource<R, S, T> {

    String getSourceName();

    void setSaveOptions(String savePath, boolean saveTotal, String format, String separator, List<String> rmFields);

    void setProcessor(ILineProcess<T> processor);

    void export(R source, S saver, ILineProcess<T> processor) throws IOException;

    void execInThread(R source, S saver, int order) throws Exception;

    void export() throws Exception;

    void setRetryTimes(int retryTimes);

    void updateSettings(CommonParams commonParams);
}
