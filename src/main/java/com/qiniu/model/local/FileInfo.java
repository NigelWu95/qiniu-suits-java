package com.qiniu.model.local;

import com.qiniu.util.Etag;
import com.qiniu.util.FileUtils;

import java.io.File;
import java.io.IOException;

public class FileInfo implements Comparable {

    public String parentPath;
    public String filepath;
    public String key;
    public String etag;
    public long length;
    public long timestamp;
    public String mime;

    public FileInfo(File file, String transferPath, int leftTrimSize) throws IOException {
        try {
            parentPath = file.getParent();
            filepath = file.getPath();
        } catch (NullPointerException e) {
            throw new IOException("file object can not be null.");
        }
        if (leftTrimSize == 0) key = filepath;
        else key = transferPath + filepath.substring(leftTrimSize);
        etag = Etag.file(file);
        length = file.length();
        timestamp = file.lastModified();
        mime = FileUtils.contentType(file);
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof FileInfo) {
            return this.filepath.compareTo(((FileInfo) o).filepath);
        } else {
            return 1;
        }
    }
}
