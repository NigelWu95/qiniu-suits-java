package com.qiniu.config;

import java.io.*;
import java.util.Properties;

public class PropertyConfig {

    private Properties properties;

    public PropertyConfig(String resourceName) throws IOException {
        InputStream inputStream = null;

        try {
            inputStream = new FileInputStream(resourceName);
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

    public String getProperty(String key) throws IOException {
        if (this.properties.containsKey(key)) {
            return this.properties.getProperty(key);
        } else {
            throw new IOException("not set " + key + " param.");
        }
    }
}
