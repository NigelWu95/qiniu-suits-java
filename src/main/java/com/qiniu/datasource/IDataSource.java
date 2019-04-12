package com.qiniu.datasource;

import com.qiniu.entry.CommonParams;
import com.qiniu.interfaces.ILineProcess;

import java.util.List;
import java.util.Map;

public interface IDataSource {

    String getSourceName();

    void setSaveOptions(String savePath, boolean saveTotal, String format, String separator, List<String> rmFields);

    void setProcessor(ILineProcess<Map<String, String>> processor);

    void export() throws Exception;

    void setRetryTimes(int retryTimes);

    void updateSettings(CommonParams commonParams);
}
