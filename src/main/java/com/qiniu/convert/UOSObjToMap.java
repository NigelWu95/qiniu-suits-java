package com.qiniu.convert;

import com.qiniu.sdk.FileItem;
import com.qiniu.util.LineUtils;

import java.io.IOException;
import java.util.Map;

public class UOSObjToMap extends Converter<FileItem, Map<String, String>> {

    private Map<String, String> indexMap;

    public UOSObjToMap(Map<String, String> indexMap) {
        this.indexMap = indexMap;
    }

    @Override
    public Map<String, String> convertToV(FileItem line) throws IOException {
        return LineUtils.getItemMap(line, indexMap);
    }

}
