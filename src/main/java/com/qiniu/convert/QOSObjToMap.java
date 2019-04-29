package com.qiniu.convert;

import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.LineUtils;

import java.io.IOException;
import java.util.*;

public class QOSObjToMap extends Converter<FileInfo, Map<String, String>> {

    public QOSObjToMap(Map<String, String> indexMap) {
        super(indexMap);
    }

    @Override
    public Map<String, String> convertToV(FileInfo line) throws IOException {
        return LineUtils.getItemMap(line, indexMap);
    }
}
