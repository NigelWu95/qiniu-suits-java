package com.qiniu.datasource;

import com.qiniu.convert.LineToMap;
import com.qiniu.convert.MapToString;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.persistence.IResultSave;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

public class LocalFileContainer extends FileContainer<BufferedReader, BufferedWriter> {

    public LocalFileContainer(String filePath, String parseType, String separator, String rmKeyPrefix, Map<String, String> indexMap,
                              int unitLen, int threads) {
        super(filePath, parseType, separator, rmKeyPrefix, indexMap, unitLen, threads);
    }

    @Override
    protected ITypeConvert<String, Map<String, String>> getNewMapConverter() throws IOException {
        return new LineToMap(parseType, separator, rmKeyPrefix, indexMap);
    }

    @Override
    protected ITypeConvert<Map<String, String>, String> getNewStringConverter() throws IOException {
        return new MapToString(saveFormat, saveSeparator, rmFields);
    }

    @Override
    public String getSourceName() {
        return "local";
    }

    @Override
    protected IResultSave<BufferedWriter> getNewResultSaver(String order) throws IOException {
        return order != null ? new FileSaveMapper(savePath, getSourceName(), order) : new FileSaveMapper(savePath);
    }

    @Override
    protected IReader<BufferedReader> getReader(String path) throws IOException {
        return new LocalFileReader(path);
    }
}
