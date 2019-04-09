package com.qiniu.convert;

import com.qcloud.cos.model.COSObjectSummary;
import com.qiniu.util.LineUtils;

import java.util.*;

public class COSObjectToMap extends ObjectToMap<COSObjectSummary> {

    public COSObjectToMap(Map<String, String> indexMap) {
        this.lineParser = line -> LineUtils.getItemMap(line, indexMap);
    }
}
