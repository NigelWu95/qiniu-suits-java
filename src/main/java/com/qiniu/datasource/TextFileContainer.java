package com.qiniu.datasource;

import com.qiniu.convert.LineToMap;
import com.qiniu.convert.MapToString;
import com.qiniu.interfaces.ITextReader;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.interfaces.IResultOutput;
import com.qiniu.util.FileUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TextFileContainer extends TextContainer<Map<String, String>> {

    private boolean autoSplit;

    public TextFileContainer(String filePath, String parseFormat, String separator, Map<String, Map<String, String>> urisMap,
                             List<String> antiPrefixes, boolean autoSplit, String addKeyPrefix, String rmKeyPrefix,
                             Map<String, String> indexMap, List<String> fields, int unitLen, int threads) throws IOException {
        super(filePath, parseFormat, separator, urisMap, antiPrefixes, addKeyPrefix, rmKeyPrefix, indexMap, fields, unitLen, threads);
        this.autoSplit = autoSplit;
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
    protected ITextReader generateReader(String name) throws IOException {
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

    // 使用 RandomAccessFile 模拟分割多个文件来处理，极端情况下，如果文件中存在相同的行，是有可能影响完整性的，虽然概率很低，但是建议存在重复行
    // 的文件最好不要使用该模拟分割的方式。
    private List<ITextReader> splitSingleFile(File file) throws IOException {
        int lineSize = FileUtils.predictLineSize(file);
        long linesNumber = file.length() / lineSize;
        if (linesNumber < threads) {
            return new ArrayList<ITextReader>(){{ add(new TextFileReader(file, null, unitLen)); }};
        }
        long avgLines = linesNumber / threads;
        long avgSize = avgLines * lineSize;
        RandomAccessFile[] accessFiles = new RandomAccessFile[threads];
        accessFiles[0] = new RandomAccessFile(file, "r");
        String endLine;
        List<ITextReader> readers = new ArrayList<>();
        int i = 1;
        for (; i < threads; i++) {
            RandomAccessFile accessFile = new RandomAccessFile(file, "r");
            accessFile.seek(i * avgSize);
            if (accessFile.readLine() == null) break;
            endLine = accessFile.readLine();
            while ("".equals(endLine)) endLine = accessFile.readLine();
            if (endLine == null) break;
            accessFiles[i] = accessFile;
            readers.add(new TextFileRandomReader(String.join("-||-", file.getName(), String.valueOf(i - 1)),
                    accessFiles[i - 1], new String(endLine.getBytes(StandardCharsets.ISO_8859_1)), unitLen));
        }
        readers.add(new TextFileRandomReader(String.join("-||-", file.getName(), String.valueOf(i - 1)),
                accessFiles[i - 1], null, unitLen));
        return readers;
    }

    @Override
    protected Stream<ITextReader> getReaders(String path) throws IOException {
        File file = new File(path);
        List<File> directories = new ArrayList<>();
        List<File> files = new ArrayList<>();
        if (file.exists()) {
            if (file.isDirectory()) {
                directories.add(file);
                Lock lock = new ReentrantLock();
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
                if (autoSplit) return splitSingleFile(file).parallelStream();
                else files.add(file);
            }
        } else {
            throw new IOException("the file not exists from path: " + path);
        }
        List<File> finalFiles;
        if (hasAntiPrefixes) {
            finalFiles = files.parallelStream()
                    .filter(pFile -> checkPrefix(pFile.getPath()))
                    .peek(pFile -> recordListerByUri(pFile.getPath()))
                    .collect(Collectors.toList());
        } else {
            files.parallelStream().forEach(pFile -> recordListerByUri(pFile.getPath()));
            finalFiles = files;
        }
        return finalFiles.parallelStream().map(pFile -> {
            try {
                return new TextFileReader(pFile, null, unitLen);
            } catch (IOException e) {
                errorLogger.error("generate reader failed by {}\t{}", pFile.getPath(), urisMap.get(pFile.getPath()), e);
                return null;
            }
        });
    }
}
