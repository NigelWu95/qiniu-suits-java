package com.qiniu.datasource;

import com.qiniu.interfaces.IFileLister;
import com.qiniu.model.local.FileInfo;
import com.qiniu.util.FileUtils;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileInfoLister implements IFileLister<FileInfo, File> {

    private final String name;
    private int limit;
    private String endPrefix;
    private List<FileInfo> fileInfoList;
    private List<File> directories;
    private Iterator<FileInfo> iterator;
    private List<FileInfo> currents;
//    private FileInfo last;
    private String lastFilePath;
    private String truncated;
    private long count;

    public FileInfoLister(File file, boolean keepDir, boolean withEtag, boolean withDatetime, boolean withMime,
                          boolean withParent, String transferPath, int leftTrimSize, String startPrefix, String endPrefix,
                          int limit) throws IOException {
        if (file == null) throw new IOException("input file is null.");
        this.name = file.getPath();
        FileFilter fileFilter;
        if ((startPrefix == null || "".equals(startPrefix)) && (endPrefix == null || "".equals(endPrefix))) {
            fileFilter = null;
        } else if (startPrefix == null || "".equals(startPrefix)) {
            fileFilter = pathname -> pathname.getPath().compareTo(endPrefix) <= 0;
        } else if (endPrefix == null || "".equals(endPrefix)) {
            fileFilter = pathname -> pathname.getPath().compareTo(startPrefix) > 0;
        } else if (startPrefix.compareTo(endPrefix) >= 0) {
            throw new IOException("start filename can not be larger than end filename prefix.");
        } else {
            fileFilter = pathname -> pathname.getPath().compareTo(startPrefix) > 0 && pathname.getPath().compareTo(endPrefix) <= 0;
        }
        File[] fs = fileFilter == null ? file.listFiles() : file.listFiles(fileFilter);
        if (fs == null) throw new IOException(file.getPath() + " is not valid directory path.");
        int initSize = fs.length > 10 ? fs.length * 2 / 3 + 1 : fs.length;
        fileInfoList = new ArrayList<>(initSize);
        if (keepDir) {
            FileInfo fileInfo = new FileInfo(file, transferPath, leftTrimSize);
            fileInfo.filepath = String.format("%s%s", fileInfo.filepath, FileUtils.pathSeparator);
            fileInfoList.add(fileInfo);
        }
        for (File f : fs) {
            if (f.isHidden()) continue;
            if (f.isDirectory()) {
                if (directories == null) directories = new ArrayList<>(initSize);
                directories.add(f);
            } else {
                fileInfoList.add(new FileInfo(f, transferPath, leftTrimSize));
            }
        }
        this.limit = limit;
        this.endPrefix = endPrefix;
        if (withEtag) {
            fileInfoList.forEach(fileInfo -> { try { fileInfo = fileInfo.withEtag(); } catch (IOException e) {
                fileInfo.etag = e.getMessage().replace("\n", ","); }});
        }
        if (withDatetime) fileInfoList.forEach(FileInfo::withDatetime);
        if (withMime) {
            fileInfoList.forEach(fileInfo -> { try { fileInfo = fileInfo.withMime(); } catch (IOException e) {
                fileInfo.mime = e.getMessage().replace("\n", ","); }});
        }
        if (withParent) fileInfoList.forEach(FileInfo::withParent);
        currents = count < limit ? new ArrayList<>((int)count) : new ArrayList<>(limit);
        fileInfoList.sort(Comparator.comparing(fileInfo -> fileInfo.filepath));
        iterator = fileInfoList.iterator();
        count = fileInfoList.size();
//        if (iterator.hasNext()) {
//            last = iterator.next();
//            iterator.remove();
//            currents.add(last);
//        }
        lastFilePath = "";
        fileFilter = null;
        file = null;
        fs = null;
    }

    public FileInfoLister(String name, List<FileInfo> fileInfoList, String startPrefix, String endPrefix, int limit) throws IOException {
        this.name = name;
        if (fileInfoList == null) throw new IOException("init fileInfoList can not be null.");
        this.fileInfoList = fileInfoList;
        this.limit = limit;
        this.endPrefix = endPrefix;
        Stream<FileInfo> stream;
        if ((startPrefix == null || "".equals(startPrefix)) && (endPrefix == null || "".equals(endPrefix))) {
            stream = fileInfoList.stream();
        } else if (startPrefix == null || "".equals(startPrefix)) {
            stream = fileInfoList.stream().filter(fileInfo -> fileInfo.filepath.compareTo(endPrefix) <= 0);
        } else if (endPrefix == null || "".equals(endPrefix)) {
            stream = fileInfoList.stream().filter(fileInfo -> fileInfo.filepath.compareTo(startPrefix) > 0);
        } else if (startPrefix.compareTo(endPrefix) >= 0) {
            throw new IOException("start filename can not be larger than end filename prefix.");
        } else {
            stream = fileInfoList.stream().filter(fileInfo ->
                    fileInfo.filepath.compareTo(startPrefix) > 0 && fileInfo.filepath.compareTo(endPrefix) <= 0);
        }
        this.fileInfoList = stream.sorted(Comparator.comparing(fileInfo -> fileInfo.filepath)).collect(Collectors.toList());
        currents = count < limit ? new ArrayList<>((int)count) : new ArrayList<>(limit);
        iterator = this.fileInfoList.iterator();
        count = this.fileInfoList.size();
//        if (iterator.hasNext()) {
//            last = iterator.next();
//            iterator.remove();
//            currents.add(last);
//        }
        lastFilePath = "";
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setEndPrefix(String endPrefix) {
        if (endPrefix != null && !"".equals(endPrefix)) {
            fileInfoList = fileInfoList.stream()
                    .filter(fileInfo -> fileInfo.filepath.compareTo(endPrefix) > 0)
                    .sorted().collect(Collectors.toList());
            iterator = fileInfoList.iterator();
            count = fileInfoList.size();
        }
    }

    @Override
    public String getEndPrefix() {
        return endPrefix;
    }

    @Override
    public void setLimit(int limit) {
        this.limit = limit;
    }

    @Override
    public synchronized void listForward() {
        if (lastFilePath == null) {
            iterator = null;
            currents.clear();
            fileInfoList.clear();
        } else if (count <= limit) {
            lastFilePath = null;
            iterator = null;
            currents = fileInfoList;
        } else {
            currents.clear();
            while (iterator.hasNext()) {
                if (currents.size() >= limit) {
                    lastFilePath = currents.get(currents.size() - 1).filepath;
                    break;
                }
                currents.add(iterator.next());
                iterator.remove();
            }
            if (!iterator.hasNext()) lastFilePath = null;
        }
    }

    @Override
    public boolean hasNext() {
        return iterator != null && iterator.hasNext();
    }

    @Override
    public List<FileInfo> currents() {
        return currents;
    }

    @Override
    public synchronized String currentEndFilepath() {
        if (truncated != null) return truncated;
        return lastFilePath;
    }

    @Override
    public List<File> getDirectories() {
        return directories;
    }

    @Override
    public List<FileInfo> getRemainedItems() {
        if (iterator == null) return null;
        return fileInfoList;
    }

    @Override
    public synchronized String truncate() {
        truncated = lastFilePath;
        lastFilePath = null;
        return truncated;
    }

    @Override
    public long count() {
        return count;
    }

    @Override
    public void close() {
        endPrefix = null;
//    private FileInfo last;
        iterator = null;
        if (currents.size() > 0) {
            lastFilePath = currents.get(currents.size() - 1).filepath;
            currents.clear();
        }
//        fileInfoList = null;
        currents = null;
    }
}
