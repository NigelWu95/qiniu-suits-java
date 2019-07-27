package com.qiniu.datasource;

import com.qiniu.convert.LineToMap;
import com.qiniu.convert.MapToString;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.persistence.IResultOutput;
import com.qiniu.util.FileUtils;

import javax.activation.MimetypesFileTypeMap;
import java.io.*;
import java.util.*;

public class LocalFileContainer extends FileContainer<BufferedReader, BufferedWriter, Map<String, String>> {

    public LocalFileContainer(String filePath, String parseFormat, String separator, String addKeyPrefix,
                              String rmKeyPrefix, Map<String, String> indexMap, int unitLen, int threads) {
        super(filePath, parseFormat, separator, addKeyPrefix, rmKeyPrefix, indexMap, unitLen, threads);
    }

    @Override
    protected ITypeConvert<String, Map<String, String>> getNewConverter() throws IOException {
        return new LineToMap(parse, separator, addKeyPrefix, rmKeyPrefix, indexMap);
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
    protected IResultOutput<BufferedWriter> getNewResultSaver(String order) throws IOException {
        return order != null ? new FileSaveMapper(savePath, getSourceName(), order) : new FileSaveMapper(savePath);
    }

    private List<File> getFiles(File directory) throws IOException {
        List<File> files = new ArrayList<>();
        for(File f : Objects.requireNonNull(directory.listFiles())) {
            if (f.isDirectory()) {
                files.addAll(getFiles(f));
            } else {
                String type = FileUtils.contentType(f);
                if (type.startsWith("text") || type.equals("application/octet-stream")) {
                    files.add(f);
                }
            }
        }
        return files;
    }

    @Override
    protected List<IReader<BufferedReader>> getFileReaders(String path) throws IOException {
        List<IReader<BufferedReader>> fileReaders = new ArrayList<>();
        path = FileUtils.realPathWithUserHome(path);
        if (path.equals(FileUtils.realPathWithUserHome(savePath))) {
            throw new IOException("the save-path can not be same as path.");
        }
        File sourceFile = new File(path);
        if (sourceFile.isDirectory()) {
            File[] fs = sourceFile.listFiles();
            if (fs == null) throw new IOException("The current path you gave may be incorrect: " + path);
            else {
                List<File> files = getFiles(sourceFile);
                for (File file : files) {
                    fileReaders.add(new LocalFileReader(file));
                }
            }
        } else {
            String type = FileUtils.contentType(sourceFile);
            if (type.startsWith("text") || type.equals("application/octet-stream")) {
                fileReaders.add(new LocalFileReader(sourceFile));
            } else {
                throw new IOException("please provide the \'text\' file. The current path you gave is: " + path);
            }
        }
        if (fileReaders.size() == 0) throw new IOException("please provide the \'text\' file in the directory. " +
                "The current path you gave is: " + path);
        return fileReaders;
    }
}
