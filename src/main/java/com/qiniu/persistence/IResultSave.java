package com.qiniu.persistence;

import java.io.*;
import java.util.HashMap;

public interface IResultSave {

    void setRetryTimes(int retryTimes);

    String getPrefix();

    String getSuffix();

    void addDefaultWriters(String writer) throws IOException;

    void initDefaultWriters() throws IOException;

    void initDefaultWriters(String targetFileDir, String prefix, String suffix) throws IOException;

    void addWriter(String key) throws IOException;

    BufferedWriter getWriter(String key);

    void closeWriters();

    void writeKeyFile(String key, String item, boolean flush) throws IOException;

    void writeSuccess(String item, boolean flush) throws IOException;

    void writeError(String item, boolean flush) throws IOException;
}
