package com.qiniu.datasource;

import com.qiniu.convert.*;
import com.qiniu.interfaces.*;
import com.qiniu.model.local.FileInfo;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.util.ConvertingUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class DefaultFileContainer extends FileContainer<FileInfo, Map<String, String>> {

    public DefaultFileContainer(String path, Map<String, Map<String, String>> directoriesMap,
                                List<String> antiDirectories, boolean keepDir, Map<String, String> indexMap,
                                List<String> fields, int unitLen, int threads) throws IOException {
        super(path, directoriesMap, antiDirectories, keepDir, indexMap, fields, unitLen, threads);
    }

    @Override
    public String getSourceName() {
        return "file";
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
        } else if ("yaml".equals(saveFormat)) {
            stringFormatter = line -> ConvertingUtils.toStringWithIndent(line, fields, initPathSize);
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
    protected IResultOutput getNewResultSaver(String order) throws IOException {
        return order != null ? new FileSaveMapper(savePath, getSourceName(), order) : new FileSaveMapper(savePath);
    }

    @Override
    protected IFileLister<FileInfo, File> getLister(File directory, String start, String end, int unitLen) throws IOException {
        return new FileInfoLister(directory, keepDir, withEtag, withDatetime, withMime, withParent, transferPath, leftTrimSize, start, end, unitLen);
    }

    @Override
    protected IFileLister<FileInfo, File> getLister(String name, List<FileInfo> fileInfoList, String start, String end,
                                                    int unitLen) throws IOException {
        return new FileInfoLister(name, fileInfoList, start, end, unitLen);
    }

    @Override
    protected IFileLister<FileInfo, File> getLister(String singleFilePath) throws IOException {
        File file = new File(singleFilePath);
        if (!file.exists()) throw new IOException(singleFilePath + " path not found.");
        FileInfo fileInfo = new FileInfo(file, transferPath, leftTrimSize);
        if (indexMap.containsKey("etag")) {
            try { fileInfo = fileInfo.withEtag(); } catch (IOException e) {
                fileInfo.etag = e.getMessage().replace("\n", ","); }
        }
        if (indexMap.containsKey("mime")) {
            try { fileInfo = fileInfo.withMime(); } catch (IOException e) {
                fileInfo.mime = e.getMessage().replace("\n", ","); }
        }
        if (indexMap.containsKey("parent")) fileInfo = fileInfo.withParent();
        List<FileInfo> list = new ArrayList<>();
        list.add(fileInfo);
        return getLister(singleFilePath, list, null, null, unitLen);
    }
}
