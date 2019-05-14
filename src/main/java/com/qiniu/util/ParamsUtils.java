package com.qiniu.util;

import java.util.*;

public class ParamsUtils {

    public final static String[] escapes = new String[]{",", "\\", ":", "="};

    public static String[] escapeSplit(String paramLine, char delimiter, String[] escaped, boolean replace) {
        if (paramLine == null || "".equals(paramLine)) return new String[0];
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
        for (int i = 0; i < elements.length; i++) {
            for (String key : escapeMap.keySet()) {
                if (elements[i].contains(key)) {
                    if (replace) elements[i] = elements[i].replace(key, escapeMap.get(key));
                    else elements[i] = elements[i].replace(key, "\\" + escapeMap.get(key));
                }
            }
        }
        return elements;
    }

    public static String[] escapeSplit(String paramLine, char delimiter) {
        return escapeSplit(paramLine, delimiter, escapes, true);
    }

    public static String[] escapeSplit(String paramLine, char delimiter, boolean replace) {
        return escapeSplit(paramLine, delimiter, escapes, replace);
    }

    public static String[] escapeSplit(String paramLine) {
        return escapeSplit(paramLine, ',', escapes, true);
    }

    public static String[] escapeSplit(String paramLine, boolean replace) {
        return escapeSplit(paramLine, ',', escapes, replace);
    }
}
