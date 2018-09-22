package com.qiniu.config;

import com.qiniu.common.QiniuSuitsException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyConfig {

    private String resourceBath;
    private Properties properties = null;

    public PropertyConfig(String resourceName) {
        resourceBath = "resources/";
        InputStream inputStream = null;

        try {
            inputStream = new FileInputStream(resourceBath + resourceName);
            properties = new Properties();
            properties.load(inputStream);
        } catch (IOException ioException) {
            QiniuSuitsException qiniuSuitsException = new QiniuSuitsException(ioException);
            qiniuSuitsException.addToFieldMap("resource", resourceName);
            qiniuSuitsException.setStackTrace(ioException.getStackTrace());
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

    public String getProperty(String key) {
        if (this.properties.containsKey(key)) {
            return this.properties.getProperty(key);
        } else {
            return "null";
        }
    }
}