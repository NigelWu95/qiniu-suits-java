package com.qiniu.config;

import com.google.gson.JsonObject;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.util.FileNameUtils;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ParamsConfig implements IEntryParam {

    private Map<String, String> paramsMap;

    public ParamsConfig(InputStream inputStream) throws IOException {
        try {
            Properties properties = new Properties();
            properties.load(new InputStreamReader(new BufferedInputStream(inputStream), "utf-8"));
            paramsMap = new HashMap<>();
            for (String key : properties.stringPropertyNames()) {
                paramsMap.put(key, properties.getProperty(key));
            }
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    inputStream = null;
                }
            }
        }
    }

    public ParamsConfig(String resourceName) throws IOException {
        this(new FileInputStream(FileNameUtils.realPathWithUserHome(resourceName)));
    }

    public ParamsConfig(JsonObject jsonObject) throws IOException {
        if (jsonObject == null || jsonObject.size() == 0)
            throw new IOException("json is empty.");
        paramsMap = new HashMap<>();
        for (String key : jsonObject.keySet()) {
            if (jsonObject.get(key).isJsonNull() || jsonObject.get(key).isJsonPrimitive() ||
                    jsonObject.get(key).isJsonObject() || jsonObject.get(key).isJsonArray()) { continue; }
            paramsMap.put(key, jsonObject.get(key).getAsString());
        }
    }

    public ParamsConfig(String[] args) throws IOException {
        if (args == null || args.length == 0)
            throw new IOException("args is empty.");
        else {
            boolean cmdGoon = false;
            paramsMap = new HashMap<>();
            String[] strings;
            for (String arg : args) {
                // 参数命令格式：-<key>=<value>
                cmdGoon = arg.matches("-.+=.+") || cmdGoon;
                if (cmdGoon) {
                    strings = splitParam(arg);
                    paramsMap.put(strings[0], strings[1]);
                }
            }
        }
    }

    public ParamsConfig(Map<String, String> initMap) throws IOException {
        if (initMap == null || initMap.size() == 0) throw new IOException("no init params.");
        this.paramsMap = initMap;
    }

    private String[] splitParam(String paramCommand) throws IOException {

        if (!paramCommand.contains("=") || !paramCommand.startsWith("-")) {
            throw new IOException("there is invalid command param: \"" + paramCommand + "\".");
        }

        String[] strings = paramCommand.substring(1).split("=");
        if (strings.length == 1) {
            // 不允许空值的出现
            throw new IOException("the \"" + strings[0] + "\" param has no value.");
        }

        if (strings[1].matches("(\".*\"|\'.*\')"))
            return new String[]{strings[0], strings[1].substring(1, strings[1].length() -1)};
        return strings;
    }

    /**
     * 获取属性值，判断是否存在相应的 key，不存在或 value 为空则抛出异常
     * @param key 属性名
     * @return 属性值字符
     * @throws IOException 无法获取参数值或者参数值为空时抛出异常
     */
    public String getValue(String key) throws IOException {
        if (paramsMap == null || "".equals(paramsMap.getOrDefault(key, ""))) {
            throw new IOException("not set \"" + key + "\" parameter.");
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
