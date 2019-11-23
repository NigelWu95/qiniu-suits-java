package com.qiniu.datasource;

import com.qiniu.model.local.FileInfo;
import com.qiniu.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FilepathLister {

    private List<FileInfo> fileInfos;
    private List<File> directories;
    private long count;

    public FilepathLister(File file, boolean checkText, String transferPath, int leftTrimSize) throws IOException {
        if (file == null) throw new IOException("input file is null.");
        File[] fs = file.listFiles();
        if (fs == null) throw new IOException("input file is not valid directory: " + file.getPath());
        fileInfos = new ArrayList<>();
        directories = new ArrayList<>();
        for(File f : fs) {
            if (f.isDirectory()) {
                directories.add(f);
            } else {
                if (checkText) {
                    String type = FileUtils.contentType(f);
                    if (type.startsWith("text") || type.equals("application/octet-stream")) {
                        fileInfos.add(new FileInfo(f, transferPath, leftTrimSize));
                    }
                } else {
                    fileInfos.add(new FileInfo(f, transferPath, leftTrimSize));
                }
            }
        }
        count += fileInfos.size();
        file = null;
    }

    public List<FileInfo> getFileInfos() {
        return fileInfos;
    }

    public List<File> getDirectories() {
        return directories;
    }

    public long count() {
        return count;
    }
}
