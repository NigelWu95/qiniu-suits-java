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

    private String name;
    private int limit;
    private String endPrefix;
    private List<FileInfo> fileInfoList;
    private List<File> directories;
    private Iterator<FileInfo> iterator;
    private List<FileInfo> currents;
    private FileInfo last;
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
        if (withEtag && withMime && withParent) {
            return stream.map(fileInfo -> {
                try {
                    return fileInfo.withEtag().withMime().withParent();
                } catch (IOException e) {
                    try {
                        fileInfo.etag = e.getMessage().replace("\n", ",");
                        return fileInfo.withMime().withParent();
                    } catch (IOException ex) {
                        fileInfo.mime = ex.getMessage().replace("\n", ",");
                        return fileInfo.withParent();
                    }
                }
            }).sorted(Comparator.comparing(fileInfo -> fileInfo.filepath)).collect(Collectors.toList());
        } else if (withEtag && withMime) {
            return stream.map(fileInfo -> {
                try {
                    return fileInfo.withEtag().withMime();
                } catch (IOException e) {
                    try {
                        fileInfo.etag = e.getMessage().replace("\n", ",");
                        return fileInfo.withMime();
                    } catch (IOException ex) {
                        fileInfo.mime = ex.getMessage().replace("\n", ",");
                        return fileInfo;
                    }
                }
            }).sorted(Comparator.comparing(fileInfo -> fileInfo.filepath)).collect(Collectors.toList());
        } else if (withEtag && withParent) {
            return stream.map(fileInfo -> {
                try {
                    return fileInfo.withEtag().withParent();
                } catch (IOException e) {
                    fileInfo.etag = e.getMessage().replace("\n", ",");
                    return fileInfo.withParent();
                }
            }).sorted(Comparator.comparing(fileInfo -> fileInfo.filepath)).collect(Collectors.toList());
        } else if (withMime && withParent) {
            return stream.map(fileInfo -> {
                try {
                    return fileInfo.withMime().withParent();
                } catch (IOException e) {
                    fileInfo.mime = e.getMessage().replace("\n", ",");
                    return fileInfo.withParent();
                }
            }).sorted(Comparator.comparing(fileInfo -> fileInfo.filepath)).collect(Collectors.toList());
        } else if (withEtag) {
            return stream.map(fileInfo -> {
                try {
                    return fileInfo.withEtag();
                } catch (IOException e) {
                    fileInfo.etag = e.getMessage().replace("\n", ",");
                    return fileInfo;
                }
            }).sorted(Comparator.comparing(fileInfo -> fileInfo.filepath)).collect(Collectors.toList());
        } else if (withMime) {
            return stream.map(fileInfo -> {
                try {
                    return fileInfo.withMime();
                } catch (IOException e) {
                    fileInfo.mime = e.getMessage().replace("\n", ",");
                    return fileInfo;
                }
            }).sorted(Comparator.comparing(fileInfo -> fileInfo.filepath)).collect(Collectors.toList());
        } else if (withParent) {
            return stream.map(FileInfo::withParent)
                    .sorted(Comparator.comparing(fileInfo -> fileInfo.filepath))
                    .collect(Collectors.toList());
        } else {
            return stream.collect(Collectors.toList());
        }
    }

    public FileInfoLister(File file, Map<String, String> indexMap, boolean checkText, String transferPath, int leftTrimSize,
                          String startPrefix, String endPrefix, int limit) throws IOException {
        if (file == null || indexMap == null) throw new IOException("input file or indexMap is null.");
        this.name = file.getPath();
        File[] fs = file.listFiles();
        if (fs == null) throw new IOException("input file is not valid directory: " + file.getPath());
        fileInfoList = new ArrayList<>(fs.length);
        directories = new ArrayList<>(fs.length);
        for(File f : fs) {
            if (f.isHidden()) continue;
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
        if (directories.size() == 0) directories = null;
        this.limit = limit;
        this.endPrefix = endPrefix;
        fileInfoList = withExtraInfo(checkFileInfoList(startPrefix), indexMap.containsKey("etag"),
                indexMap.containsKey("mime"), indexMap.containsKey("parent"));
        currents = new ArrayList<>();
        iterator = fileInfoList.iterator();
        if (iterator.hasNext()) {
            last = iterator.next();
            iterator.remove();
            currents.add(last);
        }
        count = fileInfoList.size();
        file = null;
    }

    public FileInfoLister(String name, List<FileInfo> fileInfoList, String startPrefix, String endPrefix, int limit) throws IOException {
        this.name = name;
        if (fileInfoList == null) throw new IOException("init fileInfoList can not be null.");
        this.fileInfoList = fileInfoList;
        this.limit = limit;
        this.endPrefix = endPrefix;
        fileInfoList = checkFileInfoList(startPrefix)
                .sorted(Comparator.comparing(fileInfo -> fileInfo.filepath))
                .collect(Collectors.toList());
        currents = new ArrayList<>();
        iterator = fileInfoList.iterator();
        if (iterator.hasNext()) {
            last = iterator.next();
            iterator.remove();
            currents.add(last);
        }
        count = fileInfoList.size();
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
    public void listForward() {
        if (last == null) {
            iterator = null;
            currents.clear();
            fileInfoList.clear();
        } else if (count <= limit) {
            last = null;
            iterator = null;
            currents = fileInfoList;
        } else {
            currents.clear();
            while (iterator.hasNext()) {
                if (currents.size() >= limit) break;
                currents.add(iterator.next());
                iterator.remove();
            }
            last = null;
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
    public String currentEndFilepath() {
        if (truncated != null) return truncated;
        return last.filepath;
    }

    @Override
    public List<File> getDirectories() {
        return directories;
    }

    @Override
    public List<FileInfo> getRemainedFiles() {
        if (iterator == null) return null;
        else return fileInfoList;
    }

    @Override
    public String truncate() {
        truncated = last.filepath;
        last = null;
        return truncated;
    }

    @Override
    public long count() {
        return count;
    }

    @Override
    public void close() {
        iterator = null;
        if (currents.size() > 0) {
            last = currents.get(currents.size() - 1);
            currents.clear();
        }
//        fileInfoList = null;
        currents = null;
    }
}
