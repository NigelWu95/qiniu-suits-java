package com.qiniu.service.datasource;

import com.qiniu.service.interfaces.ILineProcess;

import java.util.Map;

public interface IDataSource {

    void exportData(ILineProcess<Map<String, String>> processor);
}
