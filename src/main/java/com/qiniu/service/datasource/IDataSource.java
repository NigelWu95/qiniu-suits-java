package com.qiniu.service.datasource;

import com.qiniu.service.interfaces.ILineProcess;

import java.util.List;
import java.util.Map;

public interface IDataSource {

    void setResultSaveOptions(String format, String separator, List<String> removeFields);

    void exportData(int threads, ILineProcess<Map<String, String>> processor) throws Exception;

    default void setRetryCount(int retryCount) {}
}
