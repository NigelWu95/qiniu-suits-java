package com.qiniu.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.util.List;

public class PfopUtils {

    public static JsonObject checkPfopJson(JsonObject jsonObject, boolean scaleCheck) throws IOException {
        if (scaleCheck) {
            List<Integer> scale = JsonConvertUtils.fromJsonArray(jsonObject.get("scale").getAsJsonArray(),
                    new TypeToken<List<Integer>>(){});
            if (scale.size() < 1) {
                throw new IOException("the json-config miss \"scale\" field in \"" + jsonObject + "\".");
            } else if (scale.size() == 1) {
                JsonArray jsonArray = new JsonArray();
                jsonArray.add(scale.get(0));
                jsonArray.add(Integer.MAX_VALUE);
                jsonObject.add("scale", jsonArray);
            }
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
        String saveAs = pfopJson.get("saveas").getAsString();
        if (saveAs.contains(":")) {

        }
        String saveAsKey = saveAs.substring(saveAs.indexOf(":") + 1);
        if (saveAsKey.endsWith("$(key)")) {
            saveAs = saveAs.replace("$(key)", srcKey);
        } else if (saveAsKey.startsWith("$(key)"))
        if (saveAsKey.contains("$(key)")) {
            if (saveAsKey.contains(".")) {
                String[] nameParts = saveAsKey.split("(\\$\\(key\\)|\\.)");
                saveAsKey = FileNameUtils.addPrefixAndSuffixWithExt(nameParts[0], srcKey, nameParts[1], nameParts[2]);
            } else {
                String[] nameParts = saveAsKey.split("\\$\\(key\\)");
                saveAsKey = FileNameUtils.addPrefixAndSuffixKeepExt(nameParts[0], srcKey, nameParts[1]);
            }
            saveAs = saveAs.replace(saveAs.substring(saveAs.indexOf(":") + 1), saveAsKey);
        }
        return pfopJson.get("cmd").getAsString() + "|saveas/" + UrlSafeBase64.encodeToString(saveAs);
    }
}
