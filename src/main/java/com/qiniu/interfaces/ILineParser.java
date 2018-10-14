package com.qiniu.interfaces;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.qiniu.common.QiniuSuitsException;
import com.qiniu.util.JSONConvertUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public interface ILineParser {

    void splitLine(String line);

    void checkSplit() throws IOException;

    ArrayList<String> getItemList() throws IOException;

    ArrayList<String> getItemList(String line);

    void setItemMap(ArrayList<String> itemKey) throws IOException;

    void setItemMap(ArrayList<String> itemKey, String line);

    String toString();

    default String getByKey(String key) {

        System.out.println(toString());
        JsonObject lineJson = JSONConvertUtils.toJsonObject(toString());
        return lineJson.get(key)  == null ? null : lineJson.get(key).getAsString();
    };
}