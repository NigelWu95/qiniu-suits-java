package com.qiniu.interfaces;

import java.io.IOException;
import java.util.List;

public interface ITextReader<S> {

    String getName();

    S getOriginal();

    List<String> readLines() throws IOException;

    String currentEndLine();

    long count();

    void close();
}
