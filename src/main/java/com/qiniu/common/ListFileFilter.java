package com.qiniu.common;

import com.qiniu.storage.model.FileInfo;

import java.util.Arrays;
import java.util.List;

public class ListFileFilter {

    private List<String> keyPrefix;
    private List<String> keySuffix;
    private List<String> keyRegex;
    private long fileSizeMin;
    private long fileSizeMax;
    private long putTimeMin;
    private long putTimeMax;
    private List<String> mimeType;
    private short type;
}