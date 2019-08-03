package com.qiniu.persistence;

import java.io.*;
import java.util.List;

public interface IResultOutput<T> {

    void setRetryTimes(int retryTimes);

    String getPrefix();

    String getSuffix();

    void preAddWriter(String key);

    void addWriter(String key) throws IOException;

    void closeWriters();

    void writeToKey(String key, String item, boolean flush) throws IOException;

    void writeSuccess(String item, boolean flush) throws IOException;

    void writeError(String item, boolean flush) throws IOException;
}
