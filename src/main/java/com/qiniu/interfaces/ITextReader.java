package com.qiniu.interfaces;

import java.io.IOException;
import java.util.List;

public interface ITextReader {

    String getName();

    List<String> readLines() throws IOException;

    String currentEndLine();

    long count();

    void close();
}
