package com.qiniu.config;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.qiniu.util.FileUtils;
import com.qiniu.util.JsonUtils;

import java.io.*;
import java.util.Set;

public class JsonFile {

    private JsonObject jsonObject;

    public JsonFile(String resourceName) throws IOException {
        InputStream inputStream = null;
        try {
            File file = new File(FileUtils.convertToRealPath(resourceName));
            if (file.exists()) {
                inputStream = new FileInputStream(file);
            } else {
                inputStream = getClass().getClassLoader().getResourceAsStream(resourceName);
            }
            if (inputStream == null) throw new IOException(resourceName + " may be not exists.");
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line);
            }
            jsonObject = JsonUtils.toJsonObject(stringBuilder.toString());
        } catch (Exception e) {
            throw new IOException("load json config file failed, " + e.getMessage());
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
