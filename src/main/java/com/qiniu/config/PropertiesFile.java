package com.qiniu.config;

import com.qiniu.util.FileUtils;

import java.io.*;
import java.util.Properties;
import java.util.Set;

public class PropertiesFile {

    private Properties properties;

    public PropertiesFile(String resourceName) throws IOException {
        InputStream inputStream = null;
        try {
            File file = new File(FileUtils.convertToRealPath(resourceName));
            if (file.exists()) {
                inputStream = new FileInputStream(file);
            } else {
                inputStream = getClass().getClassLoader().getResourceAsStream(resourceName);
            }
            if (inputStream == null) throw new IOException(resourceName + " may be not exists.");
            properties = new Properties();
            properties.load(new InputStreamReader(new BufferedInputStream(inputStream), "utf-8"));
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

    public Properties getProperties() {
        return properties;
    }

    /**
     * 获取属性值，判断是否存在相应的 key，不存在或 value 为空则抛出异常
     * @param key 属性名
     * @return 属性值字符
     * @throws IOException 无法获取参数值或者参数值为空时抛出异常
     */
    public String getValue(String key) throws IOException {
        if ("".equals(properties.getProperty(key, ""))) {
            throw new IOException("not set \"" + key + "\" parameter value.");
        } else {
            return properties.getProperty(key);
        }
    }

    /**
     * 获取属性值，不抛出异常，使用 default 值进行返回
     * @param key 属性名
     * @param Default 默认返回值
     * @return 属性值字符
     */
    public String getValue(String key, String Default) {
        if ("".equals(properties.getProperty(key, ""))) return Default;
        return properties.getProperty(key, Default);
    }

    public Set<String> getKeys() {
        return properties.stringPropertyNames();
    }
}
