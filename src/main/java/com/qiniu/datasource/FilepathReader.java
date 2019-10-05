package com.qiniu.datasource;

import com.qiniu.interfaces.IReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FilepathReader implements IReader<Iterator<String>> {

    private String name;
    private Iterator<String> iterator;
    private int limit;
    private String line;
    private List<String> lineList;
    private long count;

    public FilepathReader(String name, List<String> filepathList, int limit) throws IOException {
        if (filepathList == null || filepathList.size() == 0) throw new IOException("invalid filepath list");
        this.iterator = filepathList.iterator();
        this.name = name;
        this.limit = limit;
        this.line = iterator.next();
        this.lineList = filepathList;
        this.count = filepathList.size();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Iterator<String> getRealReader() {
        return iterator;
    }

    @Override
    public List<String> readLines() {
        if (line == null) {
            return null;
        } else if (count <= limit) {
            line = null;
            return lineList;
        } else {
            List<String> srcList = new ArrayList<>();
            srcList.add(line);
            while (true) {
                if (srcList.size() >= limit) break;
                if (iterator.hasNext()) {
                    line = iterator.next();
                    srcList.add(line);
                } else {
                    line = null;
                    break;
                }
            }
            return srcList;
        }
    }

    @Override
    public String lastLine() {
        return line;
    }

    @Override
    public long count() {
        return count;
    }

    @Override
    public void close() {
        iterator = null;
    }
}
