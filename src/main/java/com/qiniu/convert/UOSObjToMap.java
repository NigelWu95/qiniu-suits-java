package com.qiniu.convert;

import com.qiniu.sdk.FolderItem;
import com.qiniu.util.LineUtils;

import java.io.IOException;
import java.util.Map;

public class UOSObjToMap extends Converter<FolderItem, Map<String, String>> {

    private Map<String, String> indexMap;

    public UOSObjToMap(Map<String, String> indexMap) {
        this.indexMap = indexMap;
    }

    @Override
    public Map<String, String> convertToV(FolderItem line) throws IOException {
        return LineUtils.getItemMap(line, indexMap);
    }

}
