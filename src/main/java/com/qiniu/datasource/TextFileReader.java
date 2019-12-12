package com.qiniu.datasource;

import com.qiniu.interfaces.ITextReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TextFileReader implements ITextReader<File> {

    private String name;
    private File source;
    private BufferedReader bufferedReader;
//    private String endPrefix;
    private int limit;
    private String line;
    private List<String> lineList; // 缓存读取的行
    private long count;

    public TextFileReader(File file, String startPrefix, int limit) throws IOException {
        FileReader fileReader;
        try {
            fileReader = new FileReader(file);
        } catch (IOException e) {
            throw new IOException("filepath may be incorrect, " + e.getMessage());
        }
        name = file.getPath();
        source = file;
        bufferedReader = new BufferedReader(fileReader);
        this.limit = limit;
        line = bufferedReader.readLine();
        if (startPrefix != null) {
            while (line != null && line.compareTo(startPrefix) <= 0) {
                line = bufferedReader.readLine();
            }
        }
        lineList = new ArrayList<>();
        if (line != null) lineList.add(line);
//        this.endPrefix = "".equals(endPrefix) ? null : endPrefix;
//        if (line != null && this.endPrefix != null && this.endPrefix.compareTo(line) >= 0) lineList.add(line);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public File getOriginal() {
        return source;
    }

    @Override
    public List<String> readLines() throws IOException {
        if (line == null) return null;
        List<String> srcList = new ArrayList<>(lineList);
        lineList.clear();
        while (true) {
            if (srcList.size() >= limit) break;
            try {
                // 避免文件过大，行数过多，使用 lines() 的 stream 方式直接转换可能会导致内存泄漏，故使用 readLine() 的方式
                line = bufferedReader.readLine();
            } catch (IOException e) {
                lineList = srcList;
                throw e;
            }
            if (line == null) break;
//            else if (endPrefix != null && endPrefix.compareTo(line) < 0) line = null;
            else srcList.add(line);
        }
        count += srcList.size();
        return srcList;
    }

    @Override
    public String currentEndLine() {
        return line;
    }

    @Override
    public long count() {
        return count;
    }

    @Override
    public void close() {
        try {
            bufferedReader.close();
        } catch (IOException e) {
            bufferedReader = null;
        }
        lineList.clear();
    }
}
