package com.qiniu.convert;

import com.qiniu.interfaces.ILineParser;
import com.qiniu.util.FileNameUtils;
import com.qiniu.util.LineUtils;

import java.io.IOException;
import java.util.*;

public class LineToMap extends Converter<String, Map<String, String>> {

    private ILineParser<String> lineParser;

    public LineToMap(String parseType, String separator, String addKeyPrefix, String rmKeyPrefix, Map<String, String> indexMap) throws IOException {
        if ("json".equals(parseType)) {
            this.lineParser = line -> process(addKeyPrefix, rmKeyPrefix, LineUtils.getItemMap(line, indexMap));
        } else if ("csv".equals(parseType)) {
            this.lineParser = line -> process(addKeyPrefix, rmKeyPrefix, LineUtils.getItemMap(line, ",", indexMap));
        } else if ("tab".equals(parseType)) {
            this.lineParser = line -> process(addKeyPrefix, rmKeyPrefix, LineUtils.getItemMap(line, separator, indexMap));
        } else {
            throw new IOException("please check your format for line to map.");
        }
    }

    private Map<String, String> process(String addKeyPrefix, String rmKeyPrefix, Map<String, String> itemMap) throws IOException {
        String key = itemMap.get("key");
        if (key != null) {
            if (addKeyPrefix == null) addKeyPrefix = "";
            itemMap.put("key", addKeyPrefix + FileNameUtils.rmPrefix(rmKeyPrefix, key));
        }
        return itemMap;
    }

    @Override
    public Map<String, String> convertToV(String line) throws IOException {
        return lineParser.getItemMap(line);
    }
}
