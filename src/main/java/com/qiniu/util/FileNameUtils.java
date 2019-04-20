package com.qiniu.util;

import java.io.IOException;

public class FileNameUtils {

    public static String realPathWithUserHome(String pathStr) throws IOException {
        if (pathStr == null || "".equals(pathStr)) throw new IOException("the path is empty.");
        if (pathStr.startsWith("~" + System.getProperty("file.separator"))) {
            return System.getProperty("user.home") + pathStr.substring(1);
        } else {
            return pathStr;
        }
    }

    public static String rmPrefix(String prefix, String name) {
        if (prefix == null || "".equals(prefix) || name.length() < prefix.length()) return name;
        return name.substring(0, prefix.length()).replace(prefix, "") + name.substring(prefix.length());
    }

    public static String addSuffix(String name, String suffix) {
        return name + suffix;
    }

    public static String addPrefix(String prefix, String name) {
        return prefix + name;
    }

    public static String addPrefixAndSuffixKeepExt(String prefix, String name, String suffix) {

        return prefix + addSuffixKeepExt(name, suffix);
    }

    public static String addSuffixKeepExt(String name, String suffix) {

        return addSuffixWithExt(name, suffix, null);
    }

    public static String addPrefixAndSuffixWithExt(String prefix, String name, String suffix, String ext) {
        return prefix + addSuffixWithExt(name, suffix, ext);
    }

    public static String replaceExt(String name, String ext) {
        return addSuffixWithExt(name, "", ext);
    }

    public static String addSuffixWithExt(String name, String suffix, String ext) {
        if (name == null) return null;
        String[] names = name.split("\\.");
        if (names.length < 2) {
            return name + suffix;
        } else {
            StringBuilder shortName = new StringBuilder();
            for (int i = 0; i < names.length - 1; i++) {
                shortName.append(names[i]).append(".");
            }
            ext = (ext == null || "".equals(ext)) ? names[names.length - 1] : ext;
            return shortName.toString().substring(0, shortName.length() - 1) + suffix + "." + ext;
        }
    }
}