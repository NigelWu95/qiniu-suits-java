package com.qiniu.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.util.List;

public final class PfopUtils {

    public static JsonObject checkPfopJson(JsonObject jsonObject, boolean scaleCheck) throws IOException {
        if (jsonObject.has("scale")) {
            List<Integer> scale = JsonUtils.fromJsonArray(jsonObject.get("scale").getAsJsonArray(),
                    new TypeToken<List<Integer>>(){});
            if (scale.size() < 1) {
                throw new IOException("the json-config's \"scale\" field is empty in \"" + jsonObject + "\".");
            } else if (scale.size() == 1) {
                JsonArray jsonArray = new JsonArray();
                jsonArray.add(scale.get(0));
                jsonArray.add(Integer.MAX_VALUE);
                jsonObject.add("scale", jsonArray);
            }
        } else if (scaleCheck) {
            throw new IOException("the json-config miss \"scale\" field in \"" + jsonObject + "\".");
        }
        if (!jsonObject.keySet().contains("cmd") || !jsonObject.keySet().contains("saveas"))
            throw new IOException("the json-config miss \"cmd\" or \"saveas\" field in \"" + jsonObject + "\".");
        else {
            String saveas = jsonObject.get("saveas").getAsString();
            if (saveas.startsWith(":") || saveas.startsWith("$("))
                throw new IOException("the json-config has incorrect \"saveas\" field without bucket in \"" + jsonObject + "\".");
            else if (saveas.endsWith(":"))
                throw new IOException("the json-config has incorrect \"saveas\" field without key format in \"" + jsonObject + "\"");
        }
        return jsonObject;
    }

    public static String generateFopCmd(String srcKey, JsonObject pfopJson) {
        return String.join("|saveas/", pfopJson.get("cmd").getAsString(),
                UrlSafeBase64.encodeToString(generateFopSaveAs(srcKey, pfopJson)));
    }

    public static String generateFopSaveAs(String srcKey, JsonObject pfopJson) {
        String saveAs = pfopJson.get("saveas").getAsString();
        if (saveAs.contains(":")) {
            String saveAsKey = saveAs.substring(saveAs.indexOf(":") + 1);
            if (saveAsKey.contains("$(name)") || saveAsKey.contains("$(ext)")) {
                String[] items = FileUtils.getNameItems(srcKey);
                saveAsKey = saveAsKey.replace("$(name)", items[0]).replace("$(ext)", items[1]);
            }
            if (saveAsKey.contains("$(key)")) {
                saveAsKey = saveAsKey.replace("$(key)", srcKey);
            }
            saveAs = saveAs.replace(saveAs.substring(saveAs.indexOf(":") + 1), saveAsKey);
        }
        return saveAs;
    }
}
