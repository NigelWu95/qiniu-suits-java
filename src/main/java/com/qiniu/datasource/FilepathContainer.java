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
        if (FileUtils.convertToRealPath(path).equals(FileUtils.convertToRealPath(savePath))) {
            throw new IOException("the save-path can not be same as path.");
        }
        List<IReader<Iterator<String>>> filepathReaders = new ArrayList<>(threads);
        String replaced = null;
        String transferPath = null;
        String realPath;
        if (path.startsWith(FileUtils.userHomeStartPath)) {
            realPath = String.join("", FileUtils.userHome, path.substring(1));
            replaced = FileUtils.userHome;
            transferPath = "~";
        } else {
            realPath = path;
        }
        if (realPath.contains("\\~")) realPath = realPath.replace("\\~", "~");
        if (realPath.endsWith(FileUtils.pathSeparator)) {
            realPath = realPath.substring(0, realPath.length() - 1);
        }
        File sourceFile = new File(realPath);
        if (sourceFile.isDirectory()) {
            List<File> files = FileUtils.getFiles(sourceFile, false);
            String filepath;
            String key;
            int size = files.size() > threads ? threads : files.size();
            List<List<String>> lists = new ArrayList<>(size);
            for (int i = 0; i < size; i++) lists.add(new ArrayList<>());
            for (int i = 0; i < files.size(); i++) {
                filepath = files.get(i).getPath();
                if (filepath.startsWith(String.join(FileUtils.pathSeparator, realPath, "."))) continue;
                if (replaced == null) key = filepath;
                else key = filepath.replace(replaced, transferPath);
                lists.get(i % size).add(String.join(separator, filepath, key));
            }
            String name;
            List<String> list;
            String startLine;
            for (int i = 0; i < size; i++) {
                name = "filepath-" + i;
                startLine = linesMap == null ? null : linesMap.get(name);
                list = lists.get(i);
                if (list.size() == 0) continue;
                filepathReaders.add(new FilepathReader(name, lists.get(i), startLine, unitLen));
            }
        } else {
            filepathReaders.add(new FilepathReader("filepath-" + path, new ArrayList<String>(){{
                add(sourceFile.getPath());
            }}, null, unitLen));
        }
        if (filepathReaders.size() == 0) throw new IOException("no files in the current path you gave: " + path);
        return filepathReaders;
    }
}
