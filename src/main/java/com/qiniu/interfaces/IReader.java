package com.qiniu.interfaces;

import java.io.IOException;
import java.util.List;

public interface IReader<T> {

    String getName();

    List<T> readLines() throws IOException;

    String lastLine();

    long count();

    void close();
}
