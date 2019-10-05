package com.qiniu.datasource;

import com.qiniu.convert.LineToMap;
import com.qiniu.convert.MapToString;
import com.qiniu.interfaces.IReader;
import com.qiniu.interfaces.IResultOutput;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.util.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.*;

public class FilepathContainer extends FileContainer<Iterator<String>, BufferedWriter, Map<String, String>> {

    public FilepathContainer(String filePath, String parseFormat, String separator, Map<String, String> linesMap,
                             Map<String, String> indexMap, List<String> fields, int unitLen, int threads) {
        super(filePath, parseFormat, separator, null, null, linesMap, indexMap, fields, unitLen, threads);
    }

    @Override
    protected ITypeConvert<String, Map<String, String>> getNewConverter() throws IOException {
        return new LineToMap(parse, separator, null, null, indexMap);
    }

    @Override
    protected ITypeConvert<Map<String, String>, String> getNewStringConverter() throws IOException {
        return new MapToString(saveFormat, saveSeparator, fields);
    }

    @Override
    public String getSourceName() {
        return "filepath";
    }

    @Override
    protected IResultOutput<BufferedWriter> getNewResultSaver(String order) throws IOException {
        return order != null ? new FileSaveMapper(savePath, getSourceName(), order) : new FileSaveMapper(savePath);
    }

    @Override
    protected List<IReader<Iterator<String>>> getFileReaders(String path) throws IOException {
        List<IReader<Iterator<String>>> filepathReaders = new ArrayList<>(threads);
        if (linesMap != null && linesMap.size() > 0) {
            try { path = FileUtils.realPathWithUserHome(path); } catch (IOException ignored) {}
            List<String> list = new ArrayList<>(linesMap.keySet());
            int size = list.size() > threads ? threads : list.size();
            List<List<String>> lists = new ArrayList<>(size);
            for (int i = 0; i < size; i++) lists.add(new ArrayList<>());
            for (int i = 0; i < list.size(); i++) {
                File file = new File(path, list.get(i));
                if (!file.exists()) file = new File(list.get(i));
                if (file.isDirectory()) throw new IOException("the filename defined in lines map can not be directory.");
                else lists.get(i % size).add(list.get(i));
            }
            for (int i = 0; i < size; i++) {
                filepathReaders.add(new FilepathReader("filepath" + i, lists.get(i), unitLen));
            }
        } else {
            path = FileUtils.realPathWithUserHome(path);
            if (path.equals(FileUtils.realPathWithUserHome(savePath))) {
                throw new IOException("the save-path can not be same as path.");
            }
            File sourceFile = new File(path);
            if (sourceFile.isDirectory()) {
                List<File> files = FileUtils.getFiles(sourceFile);
                String filepath;
                int size = files.size() > threads ? threads : files.size();
                List<List<String>> lists = new ArrayList<>(size);
                for (int i = 0; i < size; i++) lists.add(new ArrayList<>());
                for (int i = 0; i < files.size(); i++) {
                    filepath = files.get(i).getPath();
                    if (filepath.contains(FileUtils.pathSeparator + ".")) continue;
                    lists.get(i % size).add(files.get(i).getPath());
                }
                for (int i = 0; i < size; i++) {
                    filepathReaders.add(new FilepathReader("filepath" + i, lists.get(i), unitLen));
                }
            } else {
                filepathReaders.add(new FilepathReader("filepath-" + sourceFile, new ArrayList<String>(){{
                    add(sourceFile.getPath());
                }}, unitLen));
            }
        }
        if (filepathReaders.size() == 0) throw new IOException("no files in the current path you gave: " + path);
        return filepathReaders;
    }
}
