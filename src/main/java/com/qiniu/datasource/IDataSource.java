package com.qiniu.datasource;

import com.qiniu.entry.CommonParams;
import com.qiniu.interfaces.ILineProcess;

import java.util.List;
import java.util.Map;

public interface IDataSource {

    void setResultOptions(boolean saveTotal, String format, String separator, List<String> rmFields);

    void setProcessor(ILineProcess<Map<String, String>> processor);

    void export() throws Exception;

    void updateSettings(CommonParams commonParams);
}
