package com.qiniu.convert;

import com.aliyun.oss.model.OSSObjectSummary;
import com.qiniu.util.LineUtils;

import java.util.Map;

public class OSSObjToMap extends ObjectToMap<OSSObjectSummary> {

    public OSSObjToMap(Map<String, String> indexMap) {
        this.lineParser = line -> LineUtils.getItemMap(line, indexMap);
    }
}
