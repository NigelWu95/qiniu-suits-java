package com.qiniu.convert;

import com.aliyun.oss.model.OSSObjectSummary;
import com.qiniu.util.LineUtils;

import java.io.IOException;
import java.util.Map;

public class OSSObjToMap extends Converter<OSSObjectSummary, Map<String, String>> {

    private Map<String, String> indexMap;

    public OSSObjToMap(Map<String, String> indexMap) {
        this.indexMap = indexMap;
    }

    @Override
    public Map<String, String> convertToV(OSSObjectSummary line) throws IOException {
        return LineUtils.getItemMap(line, indexMap);
    }
}
