package com.qiniu.util;

import java.util.*;

public class ParamsUtils {

    public final static String[] escapes = new String[]{",", "\\", ":", "="};

    public static List<String> escapeSplit(String paramLine, char delimiter, String[] escaped) {
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
            for (String replace : escapeMap.keySet()) {
                if (element.contains(replace)) element = element.replace(replace, escapeMap.get(replace));
            }
            splitList.add(element);
        }
        return splitList;
    }

    public static List<String> escapeSplit(String paramLine, char delimiter) {
        return escapeSplit(paramLine, delimiter, escapes);
    }

    public static List<String> escapeSplit(String paramLine) {
        return escapeSplit(paramLine, ',', escapes);
    }
}
