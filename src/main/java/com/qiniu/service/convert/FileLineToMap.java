package com.qiniu.service.convert;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.qiniu.model.parameter.InfoMapParams;
import com.qiniu.service.interfaces.ILineParser;
import com.qiniu.service.interfaces.ITypeConvert;

import java.util.*;
import java.util.stream.Collectors;

public class FileLineToMap implements ITypeConvert<String, Map<String, String>> {

    private ILineParser lineParser;
    private Map<String, String> infoIndexMap;

    public FileLineToMap(String parserTye, String separator, InfoMapParams infoMapParams) {

        this.infoIndexMap = new HashMap<>();
        this.infoIndexMap.put(infoMapParams.getKeyIndex(), "key");
        this.infoIndexMap.put(infoMapParams.getHashIndex(), "hash");
        this.infoIndexMap.put(infoMapParams.getFsizeIndex(), "fsize");
        this.infoIndexMap.put(infoMapParams.getPutTimeIndex(), "putTime");
        this.infoIndexMap.put(infoMapParams.getMimeTypeIndex(), "mimeType");
        this.infoIndexMap.put(infoMapParams.getEndUserIndex(), "endUser");
        this.infoIndexMap.put(infoMapParams.getTypeIndex(), "type");
        this.infoIndexMap.put(infoMapParams.getStatusIndex(), "status");

        if ("json".equals(parserTye)) {
            lineParser = line -> {
                JsonObject parsed = new JsonParser().parse(line).getAsJsonObject();
                Map<String, String> itemMap = new HashMap<>();
                for (String key : parsed.keySet()) {
                    String mapKey = infoIndexMap.get(key);
                    if (mapKey != null) itemMap.put(mapKey, String.valueOf(parsed.get(key)));
                }
                return itemMap;
            };
        } else {
            lineParser = line -> {
                String[] items = line.split(separator);
                Map<String, String> itemMap = new HashMap<>();
                for (int i = 0; i < items.length; i++) {
                    String mapKey = infoIndexMap.get(String.valueOf(i));
                    if (mapKey != null) itemMap.put(mapKey, items[i]);
                }
                return itemMap;
            };
        }
    }

    public List<Map<String, String>> convertToVList(List<String> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        return srcList.parallelStream()
                .filter(line -> line != null && !"".equals(line))
                .map(line -> lineParser.getItemMap(line))
                .collect(Collectors.toList());
    }
}
