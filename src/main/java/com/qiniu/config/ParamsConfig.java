package com.qiniu.config;

import com.google.gson.JsonObject;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.util.ParamsUtils;

import java.io.*;
import java.util.Map;
import java.util.Properties;

public class ParamsConfig implements IEntryParam {

    private Map<String, String> paramsMap;

    public ParamsConfig(Properties properties) {
        paramsMap = ParamsUtils.toParamsMap(properties);
    }

    public ParamsConfig(String resource) throws IOException {
        paramsMap = ParamsUtils.toParamsMap(resource);
    }

    public ParamsConfig(JsonObject jsonObject) throws IOException {
        paramsMap = ParamsUtils.toParamsMap(jsonObject);
    }

    public ParamsConfig(String[] args) throws IOException {
        paramsMap = ParamsUtils.toParamsMap(args);
    }

    public ParamsConfig(Map<String, String> initMap) throws IOException {
        if (initMap == null || initMap.size() == 0) throw new IOException("no init params.");
        this.paramsMap = initMap;
    }

    public void addParams(Map<String, String> paramsMap) {
        this.paramsMap.putAll(paramsMap);
    }

    public void addParam(String key, String value) {
        this.paramsMap.put(key, value);
    }

    /**
     * 获取属性值，判断是否存在相应的 key，不存在或 value 为空则抛出异常
     * @param key 属性名
     * @return 属性值字符
     * @throws IOException 无法获取参数值或者参数值为空时抛出异常
     */
    public String getValue(String key) throws IOException {
        if (paramsMap == null || "".equals(paramsMap.getOrDefault(key, ""))) {
            throw new IOException("not set \"" + key + "\" parameter value.");
        } else {
            return paramsMap.get(key);
        }

    }

    /**
     * 获取属性值，不抛出异常，使用 default 值进行返回
     * @param key 属性名
     * @param Default 默认返回值
     * @return 属性值字符
     */
    public String getValue(String key, String Default) {
        if (paramsMap == null || "".equals(paramsMap.getOrDefault(key, ""))) {
            return Default;
        } else {
            return paramsMap.getOrDefault(key, Default);
        }
    }

    public Map<String, String> getParamsMap() {
        return paramsMap;
    }
}
