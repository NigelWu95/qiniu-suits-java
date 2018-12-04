package com.qiniu.service.fileline;

import com.qiniu.service.interfaces.ILineParser;

import java.util.*;

public class SplitLineParser implements ILineParser {

    private String separator;

    public SplitLineParser(String separator) {
        this.separator = separator;
    }

    public ArrayList<String> parseLine(String line) {
        String[] items = line.split(separator);
        return new ArrayList<>(Arrays.asList(items));
    }

    public Map<String, String> getItemMapByKeys(String line, ArrayList<String> itemKey) {
        List<String> itemList = parseLine(line);
        Map<String, String> itemMap = new HashMap<>();
        for (int i = 0; i < itemKey.size(); i++) {
            itemMap.put(itemKey.get(i), itemList.get(i));
        }

        return itemMap;
    }

    public Map<String, String> getItemMap(String line) {
        List<String> itemList = parseLine(line);
        Map<String, String> itemMap = new HashMap<>();
        for (int i = 0; i < itemList.size(); i++) {
            itemMap.put(String.valueOf(i), itemList.get(i));
        }

        return itemMap;
    }
}
