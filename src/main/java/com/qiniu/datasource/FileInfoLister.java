package com.qiniu.datasource;

import com.qiniu.interfaces.IReader;
import com.qiniu.model.local.FileInfo;
import com.qiniu.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class FileInfoLister implements IReader<FileInfo> {

    private String name;
    private Iterator<FileInfo> iterator;
    private int limit;
    private FileInfo last;
    private List<FileInfo> fileInfoList;
    private List<File> directories;
    private long count;

    public FileInfoLister(File file, boolean checkText, String transferPath, int leftTrimSize, String start,
                          String endPrefix, int limit) throws IOException {
        if (file == null) throw new IOException("input file is null.");
        this.name = file.getPath();
        File[] fs = file.listFiles();
        if (fs == null) throw new IOException("input file is not valid directory: " + file.getPath());
        fileInfoList = new ArrayList<>();
        directories = new ArrayList<>();
        for(File f : fs) {
            if (f.isDirectory()) {
                directories.add(f);
            } else {
                if (checkText) {
                    String type = FileUtils.contentType(f);
                    if (type.startsWith("text") || type.equals("application/octet-stream")) {
                        fileInfoList.add(new FileInfo(f, transferPath, leftTrimSize));
                    }
                } else {
                    fileInfoList.add(new FileInfo(f, transferPath, leftTrimSize));
                }
            }
        }
        if ((start == null || "".equals(start)) && (endPrefix == null || "".equals(endPrefix))) {
            fileInfoList.sort(Comparator.comparing(fileInfo -> fileInfo.filepath));
        } else if (start == null || "".equals(start)) {
            fileInfoList = fileInfoList.stream()
                    .filter(fileInfo -> fileInfo.filepath.compareTo(endPrefix) <= 0)
                    .sorted().collect(Collectors.toList());
        } else if (endPrefix == null || "".equals(endPrefix)) {
            fileInfoList = fileInfoList.stream()
                    .filter(fileInfo -> fileInfo.filepath.compareTo(start) > 0)
                    .sorted().collect(Collectors.toList());
        } else if (start.compareTo(endPrefix) >= 0) {
            throw new IOException("start filename can not be larger than end filename prefix.");
        } else {
            fileInfoList = fileInfoList.stream()
                    .filter(fileInfo -> fileInfo.filepath.compareTo(start) > 0 && fileInfo.filepath.compareTo(endPrefix) <= 0)
                    .sorted().collect(Collectors.toList());
        }
        this.limit = limit;
        count = fileInfoList.size();
        iterator = fileInfoList.iterator();
        last = iterator.next();
        file = null;
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
            return fileInfoList;
        } else {
            List<FileInfo> srcList = new ArrayList<>();
            while (iterator.hasNext()) {
                if (srcList.size() >= limit) return srcList;
                srcList.add(iterator.next());
                iterator.remove();
            }
            last = null;
            return srcList;
        }
    }

    public List<File> getDirectories() {
        return directories;
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
        if (fileInfoList.size() > 0) {
            last = fileInfoList.get(fileInfoList.size() - 1);
            fileInfoList.clear();
        }
    }
}
