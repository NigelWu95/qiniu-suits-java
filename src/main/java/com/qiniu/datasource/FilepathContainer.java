package com.qiniu.datasource;

import com.qiniu.convert.LineToMap;
import com.qiniu.convert.MapToString;
import com.qiniu.interfaces.IReader;
import com.qiniu.interfaces.IResultOutput;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.util.Etag;
import com.qiniu.util.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.*;

public class FilepathContainer extends FileContainer<Iterator<String>, BufferedWriter, Map<String, String>> {

    public FilepathContainer(String filePath, String parseFormat, String separator, Map<String, String> linesMap,
                             Map<String, String> indexMap, List<String> fields, int unitLen, int threads) throws IOException {
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
        String transferPath = null;
        int leftTrimSize = 0;
        String realPath;
        if (path.indexOf(FileUtils.pathSeparator + FileUtils.currentPath) > 0 ||
                path.indexOf(FileUtils.pathSeparator + FileUtils.parentPath) > 0 ||
                path.endsWith(FileUtils.pathSeparator + ".") ||
                path.endsWith(FileUtils.pathSeparator + "..")) {
            throw new IOException("please set straight path.");
        } else if (path.startsWith(FileUtils.userHomeStartPath)) {
            realPath = String.join("", FileUtils.userHome, path.substring(1));
            transferPath = "~";
            leftTrimSize = FileUtils.userHome.length();
        } else {
            realPath = path;
            if (path.startsWith(FileUtils.parentPath) || "..".equals(path)) {
                transferPath = "";
                leftTrimSize = 3;
            } else if (path.startsWith(FileUtils.currentPath) || ".".equals(path)) {
                transferPath = "";
                leftTrimSize = 2;
            }
        }
        if (realPath.contains("\\~")) realPath = realPath.replace("\\~", "~");
        if (realPath.endsWith(FileUtils.pathSeparator)) {
            realPath = realPath.substring(0, realPath.length() - 1);
        }
        File sourceFile = new File(realPath);
        if (sourceFile.isDirectory()) {
            List<File> files = FileUtils.getFiles(sourceFile, false);
            int size = files.size() > threads ? threads : files.size();
            List<List<String>> lists = new ArrayList<>(size);
            for (int i = 0; i < size; i++) lists.add(new ArrayList<>());
            File file;
            String filepath;
            String key;
            String etag;
            long length;
            long timestamp;
            String mime;
            for (int i = 0; i < files.size(); i++) {
                file = files.get(i);
                filepath = file.getPath();
                etag = Etag.file(file);
                length = file.length();
                timestamp = file.lastModified();
                mime = FileUtils.contentType(file);
//                if (filepath.startsWith(String.join(FileUtils.pathSeparator, realPath, "."))) continue;
                if (file.isHidden()) continue;
                if (leftTrimSize == 0) key = filepath;
                else key = transferPath + filepath.substring(leftTrimSize);
                lists.get(i % size).add(String.join(separator, filepath, key, etag, String.valueOf(length),
                        String.valueOf(timestamp), mime));
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
            filepathReaders.add(new FilepathReader("filepath-" + path,
                    new ArrayList<String>(){{ add(String.join(separator, sourceFile.getPath(),
                        Etag.file(sourceFile), String.valueOf(sourceFile.length()),
                        String.valueOf(sourceFile.lastModified()), FileUtils.contentType(sourceFile)));
            }}, null, unitLen));
        }
        if (filepathReaders.size() == 0) throw new IOException("no files in the current path you gave: " + path);
        return filepathReaders;
    }
}
