package com.qiniu.persistence;

import java.io.*;

public interface IResultOutput<T> {

    void setRetryTimes(int retryTimes);

    String getPrefix();

    String getSuffix();

    T getWriter(String key);

    void closeWriters();

    void writeKeyFile(String key, String item, boolean flush) throws IOException;

    void writeSuccess(String item, boolean flush) throws IOException;

    void writeError(String item, boolean flush) throws IOException;
}
