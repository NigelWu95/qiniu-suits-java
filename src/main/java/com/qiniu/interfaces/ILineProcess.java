package com.qiniu.interfaces;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface ILineProcess<T> {

    ILineProcess<T> clone() throws CloneNotSupportedException;

    String getProcessName();

    default void setBatchSize(int batchSize) throws IOException {}

    default void setRetryTimes(int retryTimes) {}

    default void updateSavePath(String savePath) throws IOException {}

    default void setNextProcessor(ILineProcess<T> nextProcessor) {}

    default ILineProcess<T> getNextProcessor() {
        return null;
    }

    String processLine(T line) throws IOException;

    void processLine(List<T> list) throws IOException;

    void closeResource();
}
