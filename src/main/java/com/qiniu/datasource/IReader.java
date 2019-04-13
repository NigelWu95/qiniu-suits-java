package com.qiniu.datasource;

import java.io.IOException;
import java.util.stream.Stream;

public interface IReader<T> {

    String getName();

    T getRealReader();

    String read() throws IOException;

    Stream<String> lines();
}
