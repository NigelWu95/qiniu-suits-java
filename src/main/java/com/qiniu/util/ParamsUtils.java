package com.qiniu.util;

import com.google.gson.JsonObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class ParamsUtils {

    public final static String[] defaultEscapes = new String[]{"\\"};

    public static String[] escapeSplit(String paramLine, char delimiter, String[] escapeArray, boolean replace) throws IOException {
        if (paramLine == null || "".equals(paramLine)) return new String[0];
        String delimit = String.valueOf(delimiter);
        if (paramLine.startsWith(delimit))
            throw new IOException("first character can not be delimiter: \"" + delimit + "\" in \"" + paramLine + "\"");
        if (paramLine.endsWith(delimit))
            throw new IOException("last character can not be delimiter: \"" + delimit + "\" in \"" + paramLine + "\"");
        String params = paramLine;
        Map<String, String> escapeMap = new HashMap<>();
        String[] escapes = new String[escapeArray.length + 1];
        escapes[0] = delimit;
        System.arraycopy(escapeArray, 0, escapes, 1, escapeArray.length);
        for (String s : escapes) {
            if (params.contains(s)) {
                if (params.contains("\\" + s)) {
                    String tempReplace = String.valueOf(System.nanoTime());
                    while (params.contains(tempReplace) && escapeMap.containsKey(tempReplace)) {
                        tempReplace = String.valueOf(System.nanoTime());
                    }
                    escapeMap.put(tempReplace, s);
                    params = params.replace("\\" + s, tempReplace);
                } else if (!delimit.equals(s) || params.contains(delimit + s)) {
                    throw new IOException("please escape \"" + s + "\" in \"" + paramLine + "\"");
                }
            }
        }

        String[] elements = params.split(delimit);
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

    public static String[] escapeSplit(String paramLine, char delimiter) throws IOException {
        return escapeSplit(paramLine, delimiter, defaultEscapes, true);
    }

    public static String[] escapeSplit(String paramLine, char delimiter, boolean replace) throws IOException {
        return escapeSplit(paramLine, delimiter, defaultEscapes, replace);
    }

    public static String[] escapeSplit(String paramLine) throws IOException {
        return escapeSplit(paramLine, ',', defaultEscapes, true);
    }

    public static String[] escapeSplit(String paramLine, boolean replace) throws IOException {
        return escapeSplit(paramLine, ',', defaultEscapes, replace);
    }

    public static Map<String, String> toParamsMap(Properties properties) {
        Map<String, String> paramsMap = new HashMap<>();
        for (String key : properties.stringPropertyNames()) {
            paramsMap.put(key, properties.getProperty(key));
        }
        return paramsMap;
    }

    public static Map<String, String> toParamsMap(String resource) throws IOException {
        resource = FileUtils.realPathWithUserHome(resource);
        FileReader fileReader = new FileReader(resource);
        BufferedReader reader = new BufferedReader(fileReader);
        Map<String, String> paramsMap = new HashMap<>();
        try {
            String line;
            String[] strings;
            while ((line = reader.readLine()) != null) {
                if (!"".equals(line) && !line.startsWith("#") && !line.startsWith("//")) {
                    strings = splitParam(line);
                    paramsMap.put(strings[0], strings[1]);
                }
            }
        } finally {
            try {
                fileReader.close();
                reader.close();
            } catch (IOException e) {
                fileReader = null;
                reader = null;
            }
        }
        return paramsMap;
    }

    public static Map<String, String> toParamsMap(JsonObject jsonObject) throws IOException {
        if (jsonObject == null || jsonObject.size() == 0) throw new IOException("json is empty.");
        Map<String, String> paramsMap = new HashMap<>();
        for (String key : jsonObject.keySet()) {
            if (jsonObject.get(key).isJsonNull() || jsonObject.get(key).isJsonPrimitive() ||
                    jsonObject.get(key).isJsonObject() || jsonObject.get(key).isJsonArray()) { continue; }
            paramsMap.put(key, jsonObject.get(key).getAsString());
        }
        return paramsMap;
    }

    public static Map<String, String> toParamsMap(String[] args, Map<String, String> preSetMap) throws IOException {
        if (args == null || args.length == 0) {
            throw new IOException("args is empty.");
        } else {
            boolean cmdGoon = false;
            Map<String, String> paramsMap = new HashMap<>();
            String[] strings;
            String key;
            for (String arg : args) {
                // 参数命令格式：-<key>=<value>
                cmdGoon = cmdGoon || arg.matches("^-.+=.+$");
                if (cmdGoon || arg.matches("^-[^=]+$")) {
                    if (!arg.startsWith("-"))
                        throw new IOException("invalid command param: \"" + arg + "\", not start with \"-\".");
                    key = arg.substring(1);
                    if (preSetMap != null && preSetMap.containsKey(key)) {
                        key = preSetMap.get(key);
                    }
                    strings = splitParam(key);
                    paramsMap.put(strings[0], strings[1]);
                }
            }
            return paramsMap;
        }
    }

    public static String[] splitParam(String paramCommand) throws IOException {
        if (!paramCommand.contains("=")) {
            throw new IOException("invalid command param: \"" + paramCommand + "\", no value set with \"=\".");
        }
        String[] strings = new String[2];
        int position = paramCommand.indexOf("="); // 取前一个 = 为赋值符号
        if (position + 1 == paramCommand.length()) {
            throw new IOException("the \"" + paramCommand + "\" param has no value."); // 不允许空值的出现
        }
        strings[0] = paramCommand.substring(0, position);
        strings[1] = paramCommand.substring(position + 1);
        if (strings[1].matches("(^\".*\"$|^\'.*\'$)")) {
            return new String[]{strings[0], strings[1].substring(1, strings[1].length() - 1)};
        }
        return strings;
    }

    public static String checked(String param, String name, String conditionReg) throws IOException {
        if (param == null || !param.matches(conditionReg)) {
            throw new IOException("no correct \"" + name + "\", please set the it conform to regex: " + conditionReg);
        }
        return param;
    }
}
