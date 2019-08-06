package com.qiniu.util;

import java.io.IOException;
import java.net.URL;

public final class URLUtils {

    public static String getKey(String url) throws IOException {
//        URL httpUrl = new URL(url);
        String path = new URL(url).getPath();
        if (url.endsWith(" ")) return (path.startsWith("/") ? path.substring(1) : path) + " ";
        else if (url.endsWith("\t")) return (path.startsWith("/") ? path.substring(1) : path) + "\t";
        else return path.startsWith("/") ? path.substring(1) : path;
    }

    public static String getEncodedURI(String uri) {
//        if (uri == null || uri.isEmpty()) return "";
//        else if (uri.endsWith(" ")) return uri.substring(0, uri.length() - 1) + "%20";
//        else if (uri.endsWith("\t")) return uri.substring(0, uri.length() - 1) + "%09";
//        else return uri;
        if (uri == null || uri.isEmpty()) return "";
        return uri.replaceAll(" ", "%20").replaceAll("\t", "%09")
                .replaceAll("\n", "%0a").replaceAll("\r", "%0d");
    }
}
