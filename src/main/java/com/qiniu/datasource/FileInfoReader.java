package com.qiniu.datasource;

import com.qiniu.interfaces.IReader;
import com.qiniu.model.local.FileInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FileInfoReader implements IReader<FileInfo> {

    private String name;
    private Iterator<FileInfo> iterator;
    private int limit;
    private FileInfo start;
    private FileInfo last;
    private List<FileInfo> lineList;
    private long count;

    public FileInfoReader(String name, List<FileInfo> fileInfoList, FileInfo startFileInfo, int limit) throws IOException {
        if (fileInfoList == null || fileInfoList.size() == 0) throw new IOException("invalid FileInfo list");
        this.name = name;
        this.iterator = fileInfoList.iterator();
        this.limit = limit;
        this.start = startFileInfo;
        this.last = iterator.next();
        this.lineList = fileInfoList;
        this.count = fileInfoList.size();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<FileInfo> readElements() {
        if (last == null) {
            return null;
        } else if (count <= limit) {
            last = null;
            if (start == null) {
                return lineList;
            } else {
                for (int i = 0; i < lineList.size(); i++) {
                    if (lineList.get(i).compareTo(start) > 0) {
                        return lineList.subList(i, lineList.size());
                    }
                }
                return null;
            }
        } else {
            List<FileInfo> srcList = new ArrayList<>();
            if (start == null || last.compareTo(start) > 0) {
                srcList.add(last);
            }
            while (true) {
                if (srcList.size() >= limit) break;
                if (iterator.hasNext()) {
                    last = iterator.next();
                    if (start == null || last.compareTo(start) > 0) {
                        srcList.add(last);
                    }
                } else {
                    last = null;
                    break;
                }
            }
            return srcList;
        }
    }

    @Override
    public String lastLine() {
        return last.filepath;
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
