package com.qiniu.util;

import java.io.IOException;
import java.net.URL;

public final class URLUtils {

    public static String getKey(String url) throws IOException {
//        URL httpUrl = new URL(url);
        String path = new URL(url).getPath();
        if (url.endsWith(" ")) {
            if (path.startsWith("/")) return String.join(" ", path.substring(1));
            else return String.join(" ", path);
        } else if (url.endsWith("\t")) {
            if (path.startsWith("/")) return String.join("\t", path.substring(1));
            else return String.join("\t", path);
        } else {
            if (path.startsWith("/")) return path.substring(1);
            else return path;
        }
    }

    public static String getEncodedURI(String uri) {
//        if (uri == null || uri.isEmpty()) return "";
//        else if (uri.endsWith(" ")) return uri.substring(0, uri.length() - 1) + "%20";
//        else if (uri.endsWith("\t")) return uri.substring(0, uri.length() - 1) + "%09";
//        else return uri;
        if (uri == null || uri.isEmpty()) return "";
        return uri.replace(" ", "%20").replace("\t", "%09")
                .replace("\n", "%0a").replace("\r", "%0d");
    }
}
