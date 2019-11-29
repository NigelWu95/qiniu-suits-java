package com.qiniu.datasource;

import com.qiniu.convert.LineToMap;
import com.qiniu.convert.MapToString;
import com.qiniu.interfaces.IReader;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.interfaces.IResultOutput;

import java.io.*;
import java.util.*;

public class TextFileContainer extends TextContainer<String, BufferedWriter, Map<String, String>> {

    public TextFileContainer(String filePath, String parseFormat, String separator, String addKeyPrefix,
                             String rmKeyPrefix, Map<String, Map<String, String>> linesMap, Map<String, String> indexMap,
                             List<String> fields, int unitLen, int threads) throws IOException {
        super(filePath, parseFormat, separator, addKeyPrefix, rmKeyPrefix, linesMap, indexMap, fields, unitLen, threads);
    }

    @Override
    protected ITypeConvert<String, Map<String, String>> getNewConverter() throws IOException {
        return new LineToMap(parse, separator, addKeyPrefix, rmKeyPrefix, indexMap);
    }

    @Override
    protected ITypeConvert<Map<String, String>, String> getNewStringConverter() throws IOException {
        return new MapToString(saveFormat, saveSeparator, fields);
    }

    @Override
    public String getSourceName() {
        return "local";
    }

    @Override
    protected IResultOutput<BufferedWriter> getNewResultSaver(String order) throws IOException {
        return order != null ? new FileSaveMapper(savePath, getSourceName(), order) : new FileSaveMapper(savePath);
    }

    @Override
    protected IReader<String> getReader(File file, String start, int unitLen) throws IOException {
        return new TextFileReader(file, start, unitLen);
    }
}
