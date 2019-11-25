package com.qiniu.datasource;

import com.qiniu.convert.*;
import com.qiniu.interfaces.*;
import com.qiniu.model.local.FileInfo;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.util.ConvertingUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.*;

public class DefaultFileContainer extends FileContainer<FileInfo, BufferedWriter, Map<String, String>> {

    public DefaultFileContainer(String path, Map<String, Map<String, String>> directoriesMap,
                                List<String> antiDirectories, Map<String, String> indexMap,
                                List<String> fields, int unitLen, int threads) throws IOException {
        super(path, directoriesMap, antiDirectories, indexMap, fields, unitLen, threads);
    }

    @Override
    protected ITypeConvert<FileInfo, Map<String, String>> getNewConverter() {
        return new Converter<FileInfo, Map<String, String>>() {
            @Override
            public Map<String, String> convertToV(FileInfo line) throws IOException {
                return ConvertingUtils.toPair(line, indexMap, new StringMapPair());
            }
        };
    }

    @Override
    protected ITypeConvert<FileInfo, String> getNewStringConverter() {
        IStringFormat<FileInfo> stringFormatter;
        if ("json".equals(saveFormat)) {
            stringFormatter = line -> ConvertingUtils.toPair(line, fields, new JsonObjectPair()).toString();
        } else {
            stringFormatter = line -> ConvertingUtils.toPair(line, fields, new StringBuilderPair(saveSeparator));
        }
        return new Converter<FileInfo, String>() {
            @Override
            public String convertToV(FileInfo line) throws IOException {
                return stringFormatter.toFormatString(line);
            }
        };
    }

    @Override
    protected ILocalFileLister<FileInfo, File> getLister(File directory, String start, String end, int unitLen) throws IOException {
        return new FileInfoLister(directory, false, transferPath, leftTrimSize, start, end, unitLen);
    }

    @Override
    public String getSourceName() {
        return "file";
    }

    @Override
    protected IResultOutput<BufferedWriter> getNewResultSaver(String order) throws IOException {
        return order != null ? new FileSaveMapper(savePath, getSourceName(), order) : new FileSaveMapper(savePath);
    }
}
