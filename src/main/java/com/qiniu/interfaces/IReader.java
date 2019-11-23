package com.qiniu.interfaces;

import java.io.IOException;
import java.util.List;

public interface IReader<T> {

    String getName();

    List<T> readElements() throws IOException;

    String lastLine();

    long count();

    void close();
}
