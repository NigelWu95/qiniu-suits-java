package com.qiniu.config;

import com.qiniu.common.QiniuSuitsException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
            properties.load(inputStream);
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