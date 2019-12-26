package com.qiniu.datasource;

import com.qiniu.interfaces.ITextReader;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class TextFileRandomReader implements ITextReader {

    private String name;
    private RandomAccessFile accessFile;
    private int limit;
    private String originalLine;
    private String line;
    private String end;
    private List<String> lineList; // 缓存读取的行
    private long count;

    public TextFileRandomReader(String name, RandomAccessFile accessFile, String end, int limit) throws IOException {
        this.name = name;
        this.accessFile = accessFile;
        this.limit = limit;
        originalLine = accessFile.readLine();
        lineList = new ArrayList<>();
//        if (line != null) lineList.add(line);
        this.end = "".equals(end) ? null : end;
        if (originalLine != null) {
            line = new String(originalLine.getBytes(StandardCharsets.ISO_8859_1));
            lineList.add(line);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<String> readLines() throws IOException {
        if (line == null) return null;
        List<String> srcList = new ArrayList<>(limit);
        srcList.addAll(lineList);
        lineList.clear();
        if (line.equals(this.end)) {
            line = null;
        } else if (end != null) {
            while (true) {
                if (srcList.size() >= limit) break;
                try {
                    // 避免文件过大，行数过多，使用 lines() 的 stream 方式直接转换可能会导致内存泄漏，故使用 readLine() 的方式
                    originalLine = accessFile.readLine();
                } catch (IOException e) {
                    lineList = srcList;
                    throw e;
                }
                if (originalLine == null) {
                    line = null;
                    break;
                } else {
                    line = new String(originalLine.getBytes(StandardCharsets.ISO_8859_1));
                    srcList.add(line);
                    if (end.equals(line)) {
                        line = null;
                        break;
                    }
                }
            }
        } else {
            while (true) {
                if (srcList.size() >= limit) break;
                try {
                    // 避免文件过大，行数过多，使用 lines() 的 stream 方式直接转换可能会导致内存泄漏，故使用 readLine() 的方式
                    originalLine = accessFile.readLine();
                } catch (IOException e) {
                    lineList = srcList;
                    throw e;
                }
                if (originalLine == null) {
                    line = null;
                    break;
                } else {
                    line = new String(originalLine.getBytes(StandardCharsets.ISO_8859_1));
                    srcList.add(line);
                }
            }
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
            accessFile.close();
        } catch (IOException e) {
            accessFile = null;
        }
        lineList.clear();
        end = null;
        originalLine = null;
    }
}
