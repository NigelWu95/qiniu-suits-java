package com.qiniu.config;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.qiniu.util.JsonConvertUtils;

import java.io.*;

public class JsonFile {

    private JsonObject jsonObject;

    public JsonFile(String resourceFile) throws IOException {
        File file = new File(resourceFile);
        Long fileLength = file.length();
        byte[] fileContent = new byte[fileLength.intValue()];
        FileInputStream inputStream = null;

        try {
            inputStream = new FileInputStream(file);
            inputStream.read(fileContent);
            jsonObject = JsonConvertUtils.toJsonObject(new String(fileContent, "UTF-8"));
        } catch (Exception e) {
            throw new IOException("load " + resourceFile + " json config failed", e);
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

    public JsonObject getJsonObject() {
        return jsonObject;
    }

    public JsonElement getElement(String key) throws IOException {
        if (jsonObject.has(key)) {
            return jsonObject.get(key);
        } else {
            throw new IOException("no member name: " + key);
        }
    }
}
