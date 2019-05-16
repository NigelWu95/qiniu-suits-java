package com.qiniu.datasource;

import com.qiniu.entry.CommonParams;
import com.qiniu.interfaces.ILineProcess;

import java.io.IOException;
import java.util.List;

public interface IDataSource<R, S, T> {

    String getSourceName();

    void setSaveOptions(String savePath, boolean saveTotal, String format, String separator, List<String> rmFields);

    void updateSettings(CommonParams commonParams);

    void export(R source, S saver, ILineProcess<T> processor) throws IOException;

    // 直接使用 export(source, saver, processor) 方法时可以不设置 processor
    default void setProcessor(ILineProcess<T> processor) {}

    // 将 source 按照 order 顺序放入线程池进行读取操作，由子类创建线程池在多线程情况下使用并实现
    default void execInThread(R source, int order) throws Exception {}

    // 根据成员变量参数直接多线程处理数据源，由子类创建线程池在需要多线程情况下使用并实现
    default void export() throws Exception {}

    default void setRetryTimes(int retryTimes) {}
}
