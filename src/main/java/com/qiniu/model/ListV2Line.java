package com.qiniu.model;

import com.qiniu.storage.model.FileInfo;

public class ListV2Line {

    public FileInfo fileInfo;

    public String marker = "";

    public String dir = "";

    public int compareTo(ListV2Line listV2Line) {
        if (listV2Line.fileInfo == null && this.fileInfo == null) return 0;
        else if (this.fileInfo == null) return 1;
        else if (listV2Line.fileInfo == null) return -1;
        else return this.fileInfo.key.compareTo(listV2Line.fileInfo.key);
    }
}