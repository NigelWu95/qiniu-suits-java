package com.qiniu.model;

import com.qiniu.storage.model.FileInfo;

import java.util.List;

public class ListResult {

    public String lastFileKey;

    public List<FileInfo> fileInfoList;

    public String nextMarker;
}