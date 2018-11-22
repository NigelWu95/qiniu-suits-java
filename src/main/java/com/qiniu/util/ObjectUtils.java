package com.qiniu.util;

public class ObjectUtils {

    public static String addSuffix(String name, String suffix) {
        return name + suffix;
    }

    public static String addPrefix(String prefix, String name) {
        return prefix + name;
    }

    public static String addSuffixKeepExt(String name, String suffix) {

        String[] names = name.split(".");
        if (names.length == 1) return name + suffix;
        else {
            StringBuilder shortName = new StringBuilder();
            for (int i = 0; i < names.length - 1; i++) {
                shortName.append(names[i]).append(".");
            }
            String ext = names[names.length - 1];
            return shortName.toString().substring(0, shortName.length() - 1) + suffix + ext;
        }
    }

    public static String addPrefixAndSuffixKeepExt(String prefix, String name, String suffix) {

        return prefix + addSuffixKeepExt(name, suffix);
    }
}