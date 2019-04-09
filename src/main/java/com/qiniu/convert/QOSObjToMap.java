package com.qiniu.convert;

import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.LineUtils;

import java.util.*;

public class QOSObjToMap extends ObjectToMap<FileInfo> {

    public QOSObjToMap(Map<String, String> indexMap) {
        this.lineParser = line -> LineUtils.getItemMap(line, indexMap);
    }
}
