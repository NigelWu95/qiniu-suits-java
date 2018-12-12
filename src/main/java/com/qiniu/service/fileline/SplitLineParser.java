package com.qiniu.service.fileline;

import com.qiniu.model.parameter.InfoMapParams;
import com.qiniu.service.interfaces.ILineParser;

import java.util.*;

public class SplitLineParser implements ILineParser {

    private String separator;
    private Map<String, String> infoIndexMap;

    public SplitLineParser(String separator, InfoMapParams infoMapParams) {
        this.separator = separator;
        this.infoIndexMap = new HashMap<>();
        this.infoIndexMap.put(infoMapParams.getKeyIndex(), "key");
        this.infoIndexMap.put(infoMapParams.getHashIndex(), "hash");
        this.infoIndexMap.put(infoMapParams.getFsizeIndex(), "fsize");
        this.infoIndexMap.put(infoMapParams.getPutTimeIndex(), "putTime");
        this.infoIndexMap.put(infoMapParams.getMimeTypeIndex(), "mimeType");
        this.infoIndexMap.put(infoMapParams.getEndUserIndex(), "endUser");
        this.infoIndexMap.put(infoMapParams.getTypeIndex(), "type");
        this.infoIndexMap.put(infoMapParams.getStatusIndex(), "status");
    }

    private ArrayList<String> parseLine(String line) {
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
