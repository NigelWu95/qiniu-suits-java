package com.qiniu.service.line;

import com.qiniu.service.interfaces.ILineParser;

import java.io.IOException;
import java.util.*;

public class SplitLineParser implements ILineParser<String> {

    private String separator;
    private Map<String, String> infoIndexMap;

    public SplitLineParser(String separator, Map<String, String> infoIndexMap) {
        this.separator = separator;
        this.infoIndexMap = infoIndexMap;
    }

    public Map<String, String> getItemMap(String line) throws IOException {
        String[] items = line.split(separator);
        Map<String, String> itemMap = new HashMap<>();
        String mapKey;
        for (int i = 0; i < items.length; i++) {
            mapKey = infoIndexMap.get(String.valueOf(i));
            if (mapKey != null) itemMap.put(mapKey, items[i]);
        }
        if (itemMap.size() < infoIndexMap.size()) throw new IOException("no enough indexes in line." +
                " The parameter indexes may have incorrect order or name.");
        return itemMap;
    }
}
