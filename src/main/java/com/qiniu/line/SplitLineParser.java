package com.qiniu.line;

import com.qiniu.interfaces.ILineParser;

import java.io.IOException;
import java.util.*;

public class SplitLineParser implements ILineParser<String> {

    private String separator;
    private Map<String, String> indexMap;

    public SplitLineParser(String separator, Map<String, String> indexMap) throws IOException {
        if (indexMap == null || indexMap.size() == 0) throw new IOException("there are no indexMap be set.");
        this.separator = separator;
        this.indexMap = indexMap;
    }

    public Map<String, String> getItemMap(String line) throws IOException {
        String[] items = line.split(separator);
        Map<String, String> itemMap = new HashMap<>();
        String mapKey;
        for (int i = 0; i < items.length; i++) {
            mapKey = indexMap.get(String.valueOf(i));
            if (mapKey != null) {
                if (items[i] == null) itemMap.put(mapKey, null);
                else itemMap.put(mapKey, items[i]);
            }
        }
        if (itemMap.size() < indexMap.size())
            throw new IOException("no enough indexes in line. The parameter indexes may have incorrect order or name.");
        return itemMap;
    }
}
