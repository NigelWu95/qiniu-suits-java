package com.qiniu.config;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.qiniu.util.FileUtils;
import com.qiniu.util.JsonUtils;

import java.io.*;
import java.util.Set;

public class JsonFile {

    private JsonObject jsonObject;

    public JsonFile(String resourceFile) throws IOException {
        resourceFile = FileUtils.realPathWithUserHome(resourceFile);
        InputStream inputStream = null;
        try {
            File file = new File(resourceFile);
            if (!file.exists()) {
                inputStream = getClass().getResourceAsStream(System.getProperty("file.separator") + resourceFile);
            } else {
                inputStream = new FileInputStream(file);
            }
            if (inputStream == null) throw new IOException(resourceFile + " may be not exists.");
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line);
            }
            jsonObject = JsonUtils.toJsonObject(stringBuilder.toString());
        } catch (Exception e) {
            throw new IOException("load " + resourceFile + " json config failed, " + e.getMessage());
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

    public Set<String> getKeys() {
        return jsonObject.keySet();
    }
}
