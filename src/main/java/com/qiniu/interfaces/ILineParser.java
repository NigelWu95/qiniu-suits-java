package com.qiniu.interfaces;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.qiniu.util.JsonConvertUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public interface ILineParser {

    void splitLine(String line);

    void checkSplit() throws IOException;

    ArrayList<String> getItemList() throws IOException;

    ArrayList<String> getItemList(String line);

    void setItemMap(ArrayList<String> itemKey) throws IOException;

    void setItemMap(ArrayList<String> itemKey, String line);

    public String toJsonString();

    default String getByKey(String key) {
        JsonObject lineJson = JsonConvertUtils.toJsonObject(toJsonString());
        return lineJson.get(key)  == null ? null : lineJson.get(key).getAsString();
    }
}
