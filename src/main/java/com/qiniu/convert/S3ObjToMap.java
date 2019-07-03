package com.qiniu.convert;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.qiniu.util.LineUtils;

import java.io.IOException;
import java.util.Map;

public class S3ObjToMap extends Converter<S3ObjectSummary, Map<String, String>> {

    private Map<String, String> indexMap;

    public S3ObjToMap(Map<String, String> indexMap) {
        this.indexMap = indexMap;
    }

    @Override
    public Map<String, String> convertToV(S3ObjectSummary line) throws IOException {
        return LineUtils.getItemMap(line, indexMap);
    }
}
