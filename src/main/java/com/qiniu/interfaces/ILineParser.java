package com.qiniu.interfaces;

import com.google.gson.JsonObject;
import com.qiniu.util.JsonConvertUtils;

import java.io.IOException;
import java.util.ArrayList;

public interface ILineParser {

    void splitLine(String line);

    void checkSplit() throws IOException;

    ArrayList<String> getItemList() throws IOException;

    ArrayList<String> getItemList(String line);

    void setItemMap(ArrayList<String> itemKey) throws IOException;

    void setItemMap(ArrayList<String> itemKey, String line);

    String toString();

    default String getByKey(String key) {
        JsonObject lineJson = JsonConvertUtils.toJsonObject(toString());
        return lineJson.get(key)  == null ? null : lineJson.get(key).getAsString();
    }
}