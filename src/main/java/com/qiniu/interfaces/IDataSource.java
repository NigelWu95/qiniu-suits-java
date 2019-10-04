package com.qiniu.interfaces;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

public interface IDataSource<R, S, T> {

    String getSourceName();

    void setSaveOptions(boolean saveTotal, String savePath, String format, String separator, List<String> rmFields) throws IOException;

    default void setRetryTimes(int retryTimes) {}

    void export(R source, S saver, ILineProcess<T> processor) throws Exception;

    // 直接使用 export(source, saver, processor) 方法时可以不设置 processor
    default void setProcessor(ILineProcess<T> processor) {}

    // 根据成员变量参数直接多线程处理数据源，由子类创建线程池在需要多线程情况下使用并实现
    default void export() throws Exception {}

    default void export(LocalDateTime startTime, long pauseDelay, long duration) throws Exception {}
}
