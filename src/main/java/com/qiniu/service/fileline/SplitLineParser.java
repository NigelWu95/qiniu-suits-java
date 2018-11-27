package com.qiniu.service.fileline;

import com.qiniu.service.interfaces.ILineParser;
import com.qiniu.util.JsonConvertUtils;

import java.io.IOException;
import java.util.*;

public class SplitLineParser implements ILineParser {

    private String separator;
    private ArrayList<String> itemList;
    private Map<String, String> itemMap;

    public SplitLineParser(String separator) {
        this.separator = separator;
    }

    public void splitLine(String line) {
        String[] items = line.split(separator);
        this.itemList = new ArrayList<>(Arrays.asList(items));
    }

    public void checkSplit() throws IOException {
        if (itemList == null) throw new IOException("has not split the line.");
    }

    public ArrayList<String> getItemList() throws IOException {
        checkSplit();
        return itemList;
    }

    public ArrayList<String> getItemList(String line) {
        if (line != null) splitLine(line);
        return itemList;
    }

    public void setItemMap(ArrayList<String> itemKey) throws IOException {
        checkSplit();
        this.itemMap = new HashMap<>();
        for (int i = 0; i < itemKey.size(); i++) {
            this.itemMap.put(itemKey.get(i), itemList.get(i));
        }
    }

    public void setItemMap(ArrayList<String> itemKey, String line) {
        if (line != null) splitLine(line);
        this.itemMap = new HashMap<>();
        for (int i = 0; i < itemKey.size(); i++) {
            this.itemMap.put(itemKey.get(i), itemList.get(i));
        }
    }

    public Map<String, String> getItemMap(String line) {
        if (itemMap == null) {
            if (line != null) splitLine(line);
            this.itemMap = new HashMap<>();
            for (int i = 0; i < itemList.size(); i++) {
                this.itemMap.put(String.valueOf(i), itemList.get(i));
            }
        }
        return itemMap;
    }

    public String toJsonString() {

        if (this.itemMap == null && this.itemList == null) {
            return "{}";
        } else if (this.itemMap == null) {
            this.itemMap = new HashMap<>();
            for (int i = 0; i < itemList.size(); i++) {
                this.itemMap.put(String.valueOf(i), itemList.get(i));
            }
        }

        return JsonConvertUtils.toJson(itemMap);
    }
}