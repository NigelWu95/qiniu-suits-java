package com.qiniu.interfaces;

import com.google.gson.JsonObject;
import com.qiniu.common.QiniuSuitsException;
import com.qiniu.util.JSONConvertUtils;

public interface ILineParser {

    void splitLine(String line) throws QiniuSuitsException;

    String toString();

    default String getByKey(String key) {

        JsonObject lineJson = JSONConvertUtils.toJson(toString());
        return lineJson.get(key).getAsString();
    };
}