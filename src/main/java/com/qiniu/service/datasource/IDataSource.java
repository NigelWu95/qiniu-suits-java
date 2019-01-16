package com.qiniu.service.datasource;

import com.qiniu.service.interfaces.ILineProcess;

import java.util.List;
import java.util.Map;

public interface IDataSource {

    void setResultSaveOptions(boolean saveTotal, String format, String separator, List<String> removeFields);

    void export(int threads, ILineProcess<Map<String, String>> processor) throws Exception;
}
