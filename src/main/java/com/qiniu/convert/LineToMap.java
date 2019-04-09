package com.qiniu.convert;

import com.qiniu.util.LineUtils;

import java.io.IOException;
import java.util.*;

public class LineToMap extends ObjectToMap<String> {

    public LineToMap(String parseType, String separator, String rmPrefix, Map<String, String> indexMap) throws IOException {
        if ("json".equals(parseType)) {
            this.lineParser = line -> process(rmPrefix, LineUtils.getItemMap(line, indexMap, false));
        } else if ("csv".equals(parseType)) {
            this.lineParser = line -> process(rmPrefix, LineUtils.getItemMap(line, ",", indexMap, false));
        } else if ("tab".equals(parseType)) {
            this.lineParser = line -> process(rmPrefix, LineUtils.getItemMap(line, separator, indexMap, false));
        } else {
            throw new IOException("please check your format for line to map.");
        }
    }

    private Map<String, String> process(String rmPrefix, Map<String, String> itemMap) {
        String key = itemMap.get("key");
        if (key != null && key.length() >= rmPrefix.length())
            itemMap.put("key", key.substring(0, rmPrefix.length()).replace(rmPrefix, "") + key.substring(rmPrefix.length()));
        return itemMap;
    }
}
