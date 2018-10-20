package com.qiniu.config;

import java.io.*;
import java.util.Properties;

public class PropertyConfig {

    private String resourceBath;
    private Properties properties;

    public PropertyConfig(String resourceName) throws Exception {
        resourceBath = "resources/";
        InputStream inputStream = null;

        try {
            inputStream = new FileInputStream(resourceBath + resourceName);
            properties = new Properties();
            properties.load(new InputStreamReader(new BufferedInputStream(inputStream), "utf-8"));
        } catch (IOException ioException) {
            throw new Exception(ioException);
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

    public String getProperty(String key) throws Exception {
        if (this.properties.containsKey(key)) {
            return this.properties.getProperty(key);
        } else {
            throw new Exception("not set " + key + " param.");
        }
    }
}