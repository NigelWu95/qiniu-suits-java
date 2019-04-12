package com.qiniu.util;

import com.google.gson.JsonObject;

public class PfopUtils {

    public static String generateFopCmd(String srcKey, JsonObject pfopJson) {
        String saveAs = pfopJson.get("saveas").getAsString();
        String saveAsKey = saveAs.substring(saveAs.indexOf(":") + 1);
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
