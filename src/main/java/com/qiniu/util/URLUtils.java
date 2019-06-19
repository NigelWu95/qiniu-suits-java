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
}
