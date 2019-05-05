package com.qiniu.util;

import java.io.IOException;
import java.net.URL;

public class URLUtils {

    public static String getKey(String url) throws IOException {
        URL httpUrl = new URL(url);
        return httpUrl.getPath().startsWith("/") ? httpUrl.getPath().substring(1) : httpUrl.getPath();
    }
}
