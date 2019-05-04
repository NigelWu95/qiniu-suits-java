package com.qiniu.convert;

import com.qcloud.cos.model.COSObjectSummary;
import com.qiniu.util.LineUtils;

import java.io.IOException;
import java.util.*;

public class COSObjToMap extends Converter<COSObjectSummary, Map<String, String>> {

    private Map<String, String> indexMap;

    public COSObjToMap(Map<String, String> indexMap) {
        this.indexMap = indexMap;
    }

    @Override
    public Map<String, String> convertToV(COSObjectSummary line) throws IOException {
        return LineUtils.getItemMap(line, indexMap);
    }

}
