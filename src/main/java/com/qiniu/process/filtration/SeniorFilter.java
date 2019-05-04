package com.qiniu.process.filtration;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.qiniu.config.JsonFile;
import com.qiniu.util.JsonConvertUtils;

import java.io.IOException;
import java.util.*;

public abstract class SeniorFilter<T> {

    final private String checkName;
    private Set<String> extMimeList;
    private Set<String> extMimeTypeList;
    private static List<String> checkList = new ArrayList<String>(){{
        add("ext-mime");
    }};

    public SeniorFilter(String checkName, String configPath, boolean rewrite) throws IOException {
        this.checkName = checkName;
        this.extMimeList = new HashSet<>();
        this.extMimeTypeList = new HashSet<>();
        if (configPath != null && !"".equals(configPath)) {
            JsonFile customJson = new JsonFile(configPath);
            JsonElement jsonElement = customJson.getElement("ext-mime");
            if (jsonElement instanceof JsonArray) this.extMimeTypeList = new HashSet<>(
                    JsonConvertUtils.fromJsonArray(jsonElement.getAsJsonArray(), new TypeToken<List<String>>(){})
            );
        }
        if (checkMime() && !rewrite) {
            JsonFile jsonFile = new JsonFile("resources" + System.getProperty("file.separator") + "check.json");
            JsonObject extMime = jsonFile.getElement("ext-mime").getAsJsonObject();
            List<String> defaultList = JsonConvertUtils.fromJsonArray(extMime.get("image").getAsJsonArray(),
                    new TypeToken<List<String>>(){});
            defaultList.addAll(JsonConvertUtils.fromJsonArray(extMime.get("audio").getAsJsonArray(),
                    new TypeToken<List<String>>(){}));
            defaultList.addAll(JsonConvertUtils.fromJsonArray(extMime.get("video").getAsJsonArray(),
                    new TypeToken<List<String>>(){}));
            this.extMimeList.addAll(defaultList);
            this.extMimeTypeList.addAll(JsonConvertUtils.fromJsonArray(extMime.get("other").getAsJsonArray(),
                    new TypeToken<List<String>>(){}));
        }
    }

    public String getCheckName() {
        return checkName;
    }

    public boolean checkMime() {
        return "ext-mime".equals(checkName);
    }

    public boolean isValid() {
        return checkList.contains(checkName);
    }

    public List<T> checkMimeType(List<T> lineList) {
        String key;
        List<T> filteredList = new ArrayList<>();
        for (T line : lineList) {
            key = valueFrom(line, "key");
            if (key != null && key.contains(".")) {
                String finalKeyMimePair = key.substring(key.lastIndexOf(".") + 1) + ":" + valueFrom(line, "mimeType");
                if (extMimeList.parallelStream().anyMatch(extMime ->
                        finalKeyMimePair.split("/")[0].equalsIgnoreCase(extMime))) {
                    break;
                }
                if (extMimeTypeList.parallelStream().noneMatch(extMime -> finalKeyMimePair.startsWith(extMime) ||
                        finalKeyMimePair.equalsIgnoreCase(extMime))) {
                    filteredList.add(line);
                }
            }
        }
        return filteredList;
    }

    public boolean checkMimeType(T line) {
        if (valueFrom(line, "key") != null && valueFrom(line, "key").contains(".")) {
            String finalKeyMimePair = valueFrom(line, "key").substring(valueFrom(line, "key")
                    .lastIndexOf(".") + 1) + ":" + valueFrom(line, "mimeType");
            if (extMimeList.parallelStream().anyMatch(extMime ->
                    finalKeyMimePair.split("/")[0].equalsIgnoreCase(extMime))) {
                return false;
            }
            return extMimeTypeList.parallelStream().noneMatch(extMime -> finalKeyMimePair.startsWith(extMime) ||
                    finalKeyMimePair.equalsIgnoreCase(extMime));
        }
        return false;
    }

    protected abstract String valueFrom(T item, String key);
}
