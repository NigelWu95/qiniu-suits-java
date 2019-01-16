package com.qiniu.service.line;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.qiniu.service.interfaces.ILineParser;

import java.io.IOException;
import java.util.*;

public class JsonStrParser implements ILineParser<String> {

    private JsonObjParser jsonObjParser;

    public JsonStrParser(Map<String, String> indexMap) {
        this.jsonObjParser = new JsonObjParser(indexMap);
    }

    public Map<String, String> getItemMap(String line) throws IOException {
        JsonObject parsed = new JsonParser().parse(line).getAsJsonObject();
        return jsonObjParser.getItemMap(parsed);
    }
}
