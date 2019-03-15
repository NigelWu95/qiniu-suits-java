package com.qiniu.service.line;

import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.qiniu.service.interfaces.ILineParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JsonObjParser implements ILineParser<JsonObject>, Cloneable {

    private HashMap<String, String> indexMap;
    private boolean forceParse;

    public JsonObjParser(HashMap<String, String> indexMap, boolean forceParse) throws IOException {
        if (indexMap == null || indexMap.size() == 0) throw new IOException("there are no indexMap be set.");
        this.indexMap = indexMap;
        this.forceParse = forceParse;
    }

    public Map<String, String> getItemMap(JsonObject json) throws IOException {
        Map<String, String> itemMap = new HashMap<>();
        String mapKey;
        for (String key : json.keySet()) {
            mapKey = indexMap.get(key);
            if (mapKey != null) {
                if (json.get(key) instanceof JsonNull) itemMap.put(mapKey, null);
                else itemMap.put(mapKey, json.get(key).getAsString());
            }
        }
        if (!forceParse && itemMap.size() < indexMap.size())
            throw new IOException("no enough indexes in line. The parameter indexes may have incorrect order or name.");
        return itemMap;
    }

    @SuppressWarnings("unchecked")
    public JsonObjParser clone() throws CloneNotSupportedException {
        JsonObjParser jsonObjParser = (JsonObjParser)super.clone();
        jsonObjParser.indexMap = (HashMap<String, String>) indexMap.clone();
        return jsonObjParser;
    }
}
