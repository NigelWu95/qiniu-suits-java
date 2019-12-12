package com.qiniu.model.local;

import com.qiniu.util.Etag;
import com.qiniu.util.FileUtils;

import java.io.File;
import java.io.IOException;

public class FileInfo implements Comparable {

    private File file;
    public String parentPath;
    public String filepath;
    public String key;
    public long length;
    public long timestamp;
    public String mime;
    public String etag;

    public FileInfo() {}

    public FileInfo(File file, String transferPath, int leftTrimSize) throws IOException {
        try {
//            parentPath = file.getParent();
            filepath = file.getPath();
        } catch (NullPointerException e) {
            throw new IOException("file object can not be null.");
        }
        this.file = file;
        if (leftTrimSize == 0) {
            if (file.isDirectory()) key = filepath + "/";
            else key = filepath;
        } else {
            if (file.isDirectory()) key = String.format("%s%s/", transferPath, filepath.substring(leftTrimSize));
            else key = transferPath + filepath.substring(leftTrimSize);
        }
        length = file.length();
//        timestamp = file.lastModified();
//        mime = FileUtils.contentType(file);
//        etag = Etag.file(file);
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof FileInfo) {
            return this.filepath.compareTo(((FileInfo) o).filepath);
        } else {
            return 1;
        }
    }

    public FileInfo withEtag() throws IOException {
        etag = Etag.file(file);
        return this;
    }

    public FileInfo withDatetime() {
        timestamp = file.lastModified();
        return this;
    }

    public FileInfo withMime() throws IOException {
        mime = FileUtils.contentType(file);
        return this;
    }

    public FileInfo withParent() {
        parentPath = file.getParent();
        return this;
    }

    public File getFile() {
        return file;
    }
}
