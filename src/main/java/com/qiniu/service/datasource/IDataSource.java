package com.qiniu.service.datasource;

import com.qiniu.service.interfaces.ILineProcess;

import java.util.List;
import java.util.Map;

public interface IDataSource {

    void setResultOptions(boolean saveTotal, String format, String separator, List<String> rmFields);

    void export(int threads, ILineProcess<Map<String, String>> processor) throws Exception;
}
