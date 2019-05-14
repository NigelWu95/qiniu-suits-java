package com.qiniu.util;

import java.util.*;

public class ParamsUtils {

    public final static String[] escapes = new String[]{",", "\\", ":", "="};

    public static List<String> escapeSplit(String paramLine, char delimiter, String[] escaped, boolean replace) {
        List<String> splitList = new ArrayList<>();
        if (paramLine == null || "".equals(paramLine)) return splitList;

        Map<String, String> escapeMap = new HashMap<>();
        for (String s : escaped) {
            if (paramLine.contains("\\" + s)) {
                String tempReplace = String.valueOf(System.nanoTime());
                while (paramLine.contains(tempReplace) && escapeMap.containsKey(tempReplace)) {
                    tempReplace = String.valueOf(System.nanoTime());
                }
                escapeMap.put(tempReplace, s);
                paramLine = paramLine.replace("\\" + s, tempReplace);
            }
        }

        String[] elements = paramLine.split(String.valueOf(delimiter));
        for (String element : elements) {
            for (String key : escapeMap.keySet()) {
                if (element.contains(key)) {
                    if (replace) element = element.replace(key, escapeMap.get(key));
                    else element = element.replace(key, "\\" + escapeMap.get(key));
                }
            }
            splitList.add(element);
        }
        return splitList;
    }

    public static List<String> escapeSplit(String paramLine, char delimiter) {
        return escapeSplit(paramLine, delimiter, escapes, true);
    }

    public static List<String> escapeSplit(String paramLine, char delimiter, boolean replace) {
        return escapeSplit(paramLine, delimiter, escapes, replace);
    }

    public static List<String> escapeSplit(String paramLine) {
        return escapeSplit(paramLine, ',', escapes, true);
    }

    public static List<String> escapeSplit(String paramLine, boolean replace) {
        return escapeSplit(paramLine, ',', escapes, replace);
    }
}
