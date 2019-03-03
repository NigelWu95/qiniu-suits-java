package com.qiniu.service.filtration;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.qiniu.config.JsonFile;
import com.qiniu.util.JsonConvertUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SeniorChecker {

    final private String checkName;
    final private List<String> extMimeList;
    final private List<String> extMimeTypeList;

    public SeniorChecker(String checkName) throws IOException {
        this.checkName = checkName;
        JsonFile jsonFile = new JsonFile("check.json");
        JsonElement jsonElement = jsonFile.getElement("ext-mime");
        JsonObject extMime = jsonElement.getAsJsonObject();
        List<String> list = JsonConvertUtils.fromJsonArray(extMime.get("image").getAsJsonArray());
        list.addAll(JsonConvertUtils.fromJsonArray(extMime.get("audio").getAsJsonArray()));
        list.addAll(JsonConvertUtils.fromJsonArray(extMime.get("video").getAsJsonArray()));
        this.extMimeList = list;
        this.extMimeTypeList = JsonConvertUtils.fromJsonArray(extMime.get("other").getAsJsonArray());
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
