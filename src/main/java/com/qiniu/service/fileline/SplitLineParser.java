package com.qiniu.service.fileline;

import com.qiniu.service.interfaces.ILineParser;

import java.io.IOException;
import java.util.*;

public class SplitLineParser implements ILineParser {

    private String separator;
    private Map<String, String> infoIndexMap;

    public SplitLineParser(String separator, Map<String, String> infoIndexMap) {
        this.separator = separator;
        this.infoIndexMap = infoIndexMap;
    }

    public Map<String, String> getItemMap(String line) throws IOException {
        String[] items = line.split(separator);
        Map<String, String> itemMap = new HashMap<>();
        for (int i = 0; i < items.length; i++) {
            String mapKey = infoIndexMap.get(String.valueOf(i));
            if (mapKey != null) itemMap.put(mapKey, items[i]);
        }
        if (itemMap.size() == 0) throw new IOException();
        return itemMap;
    }
}
