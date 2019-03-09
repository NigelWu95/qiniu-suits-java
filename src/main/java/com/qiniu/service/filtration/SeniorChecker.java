package com.qiniu.service.filtration;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.qiniu.config.JsonFile;
import com.qiniu.util.JsonConvertUtils;

import java.io.IOException;
import java.util.*;

public class SeniorChecker {

    final private String checkName;
    final private Set<String> extMimeList;
    private Set<String> extMimeTypeList;

    public SeniorChecker(String checkName, String configPath, boolean rewrite) throws IOException {
        this.checkName = checkName;
        this.extMimeList = new HashSet<>();
        this.extMimeTypeList = new HashSet<>();
        if (configPath != null && !"".equals(configPath)) {
            JsonFile customJson = new JsonFile(configPath);
            JsonElement jsonElement = customJson.getElement("ext-mime");
            this.extMimeTypeList = new HashSet<>(JsonConvertUtils.fromJsonArray(jsonElement.getAsJsonArray(),
                    new TypeToken<List<String>>(){}));
        }
        if (!rewrite) {
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

    public boolean isValid() {
        return checkName != null && !"".equals(checkName);
    }

    public List<Map<String, String>> checkMimeType(List<Map<String, String>> lineList) {
        String key;
        List<Map<String, String>> filteredList = new ArrayList<>();
        for (Map<String, String> line : lineList) {
            key = line.get("key");
            if (key != null && key.contains(".")) {
                String finalKeyMimePair = key.substring(key.lastIndexOf(".") + 1) + ":" + line.get("mimeType");
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

    public boolean checkMimeType(Map<String, String> line) {
        if (line.get("key") != null && line.get("key").contains(".")) {
            String finalKeyMimePair = line.get("key").substring(line.get("key").lastIndexOf(".") + 1) +
                    ":" + line.get("mimeType");
            if (extMimeList.parallelStream().anyMatch(extMime ->
                    finalKeyMimePair.split("/")[0].equalsIgnoreCase(extMime))) {
                return false;
            }
            return extMimeTypeList.parallelStream().noneMatch(extMime -> finalKeyMimePair.startsWith(extMime) ||
                    finalKeyMimePair.equalsIgnoreCase(extMime));
        }
        return false;
    }
}
