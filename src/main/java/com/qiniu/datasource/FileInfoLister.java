package com.qiniu.datasource;

import com.qiniu.interfaces.ILocalFileLister;
import com.qiniu.model.local.FileInfo;
import com.qiniu.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileInfoLister implements ILocalFileLister<FileInfo, File> {

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

    private Stream<FileInfo> checkFileInfoList(String startPrefix) throws IOException {
        if ((startPrefix == null || "".equals(startPrefix)) && (endPrefix == null || "".equals(endPrefix))) {
            return fileInfoList.stream();
        } else if (startPrefix == null || "".equals(startPrefix)) {
            return fileInfoList.stream().filter(fileInfo -> fileInfo.filepath.compareTo(endPrefix) <= 0);
        } else if (endPrefix == null || "".equals(endPrefix)) {
            return fileInfoList.stream().filter(fileInfo -> fileInfo.filepath.compareTo(startPrefix) > 0);
        } else if (startPrefix.compareTo(endPrefix) >= 0) {
            throw new IOException("start filename can not be larger than end filename prefix.");
        } else {
            return fileInfoList.stream().filter(fileInfo ->
                    fileInfo.filepath.compareTo(startPrefix) > 0 && fileInfo.filepath.compareTo(endPrefix) <= 0);
        }
    }

    private List<FileInfo> withExtraInfo(Stream<FileInfo> stream, boolean withEtag, boolean withMime, boolean withParent) {
        if (withEtag) {
            stream = stream.map(fileInfo -> { try { return fileInfo.withEtag(); } catch (IOException e) {
                fileInfo.etag = e.getMessage().replace("\n", ","); return fileInfo; }});
        }
        if (withMime) {
            stream = stream.map(fileInfo -> { try { return fileInfo.withMime(); } catch (IOException e) {
                fileInfo.mime = e.getMessage().replace("\n", ","); return fileInfo; }});
        }
        if (withParent) {
            stream = stream.map(FileInfo::withParent);
        }
        return stream.sorted(Comparator.comparing(fileInfo -> fileInfo.filepath)).collect(Collectors.toList());
    }

    public FileInfoLister(File file, Map<String, String> indexMap, boolean keepDir, String transferPath, int leftTrimSize,
                          String startPrefix, String endPrefix, int limit) throws IOException {
        if (file == null || indexMap == null) throw new IOException("input file or indexMap is null.");
        this.name = file.getPath();
        File[] fs = file.listFiles();
        if (fs == null) throw new IOException("input file is not valid directory: " + file.getPath());
        fileInfoList = new ArrayList<>(fs.length);
        directories = new ArrayList<>(fs.length);
        if (keepDir) {
            FileInfo fileInfo;
            for(File f : fs) {
                if (f.isHidden()) continue;
                if (f.isDirectory()) {
                    directories.add(f);
                    fileInfo = new FileInfo(f, transferPath, leftTrimSize);
                    fileInfo.filepath = String.format("%s%s", fileInfo.filepath, FileUtils.pathSeparator);
                } else {
                    fileInfo = new FileInfo(f, transferPath, leftTrimSize);
                }
                fileInfoList.add(fileInfo);
            }
        } else {
            for (File f : fs) {
                if (f.isHidden()) continue;
                if (f.isDirectory()) {
                    directories.add(f);
                } else {
                    fileInfoList.add(new FileInfo(f, transferPath, leftTrimSize));
                }
            }
        }
        if (directories.size() == 0) directories = null;
        this.limit = limit;
        this.endPrefix = endPrefix;
        fileInfoList = withExtraInfo(checkFileInfoList(startPrefix), indexMap.containsKey("etag"),
                indexMap.containsKey("mime"), indexMap.containsKey("parent"));
        currents = new ArrayList<>();
        iterator = fileInfoList.iterator();
        count = fileInfoList.size();
//        if (iterator.hasNext()) {
//            last = iterator.next();
//            iterator.remove();
//            currents.add(last);
//        }
        lastFilePath = "";
        file = null;
    }

    public FileInfoLister(String name, List<FileInfo> fileInfoList, String startPrefix, String endPrefix, int limit) throws IOException {
        this.name = name;
        if (fileInfoList == null) throw new IOException("init fileInfoList can not be null.");
        this.fileInfoList = fileInfoList;
        this.limit = limit;
        this.endPrefix = endPrefix;
        this.fileInfoList = checkFileInfoList(startPrefix)
                .sorted(Comparator.comparing(fileInfo -> fileInfo.filepath))
                .collect(Collectors.toList());
        currents = new ArrayList<>();
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
            lastFilePath = null;
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
    public List<FileInfo> getRemainedFiles() {
        if (iterator == null) fileInfoList.clear();
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
