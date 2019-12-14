package com.qiniu.datasource;

import com.qiniu.convert.LineToMap;
import com.qiniu.convert.MapToString;
import com.qiniu.interfaces.ITextReader;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.interfaces.IResultOutput;
import com.qiniu.util.FileUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TextFileContainer extends TextContainer<File, Map<String, String>> {

    public TextFileContainer(String filePath, String parseFormat, String separator, Map<String, Map<String, String>> urisMap,
                             List<String> antiPrefixes, String addKeyPrefix, String rmKeyPrefix, Map<String, String> indexMap,
                             List<String> fields, int unitLen, int threads) throws IOException {
        super(filePath, parseFormat, separator, urisMap, antiPrefixes, addKeyPrefix, rmKeyPrefix, indexMap, fields, unitLen, threads);
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
    protected IResultOutput getNewResultSaver(String order) throws IOException {
        return order != null ? new FileSaveMapper(savePath, getSourceName(), order) : new FileSaveMapper(savePath);
    }

    @Override
    protected ITextReader<File> generateReader(String name) throws IOException {
        Map<String, String> map = urisMap.get(name);
        File file = new File(name);
        if (!file.exists()) file = new File(path, name);
        if (file.isDirectory()) {
            throw new IOException(name + " is a directory, but it should be a file.");
        } else if (file.exists()) {
            return new TextFileReader(file, map == null ? null : map.get("start"), unitLen);
        } else {
            throw new IOException(name + " is not exists.");
        }
    }

    @Override
    protected String getUriFrom(File file) {
        return file.getPath();
    }

    @Override
    protected ITextReader<File> generateReader(File file) throws IOException {
        Map<String, String> map = urisMap.get(file.getPath());
        return new TextFileReader(file, map == null ? null : map.get("start"), unitLen);
    }

    private Lock lock = new ReentrantLock();

    @Override
    protected List<File> getUriEntities(String path) throws IOException {
        File file = new File(path);
        List<File> files = new ArrayList<>();
        List<File> directories = new ArrayList<>();
        if (file.exists()) {
            if (file.isDirectory()) {
                directories.add(file);
                while (directories.size() > 0) {
                    directories = directories.parallelStream().map(directory -> {
                        File[] listFiles = directory.listFiles();
                        if (listFiles == null) return null;
                        List<File> fs = new ArrayList<>(listFiles.length);
                        List<File> dirs = new ArrayList<>(listFiles.length);
                        for (File f : listFiles) {
                            if (f.isDirectory()) {
                                dirs.add(f);
                            } else {
                                String type = FileUtils.contentType(f);
                                if (type.startsWith("text") || type.equals("application/octet-stream")) {
                                    fs.add(f);
                                }
                            }
                        }
                        while (!lock.tryLock());
                        files.addAll(fs);
                        lock.unlock();
                        return dirs;
                    }).filter(Objects::nonNull)
                            .reduce((list1, list2) -> { list1.addAll(list2); return list1; }).orElse(new ArrayList<>());
                }
            } else {
                files.add(file);
            }
        } else {
            throw new IOException("the file not exists from path: " + path);
        }
        return files;
    }
}
