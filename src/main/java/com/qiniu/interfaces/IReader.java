package com.qiniu.interfaces;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

public interface IReader<T> {

    String getName();

    T getRealReader();

    String readLine() throws IOException;

    List<String> readLines() throws IOException;

    boolean isTruncated();

    Stream<String> lines();

    void close();
}
