package com.qiniu.interfaces;

import java.io.IOException;
import java.util.stream.Stream;

public interface IReader<T> {

    String getName();

    T getRealReader();

    String readLine() throws IOException;

    Stream<String> lines();

    void close();
}
