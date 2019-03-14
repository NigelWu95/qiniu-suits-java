package com.qiniu.util;

import java.io.IOException;
import java.net.URL;

public class URLUtils {

    public static String getKey(String url) throws IOException {
        if (url != null) {
            URL httpUrl = new URL(url);
            return httpUrl.getPath().startsWith("/") ? httpUrl.getPath().substring(1) : httpUrl.getPath();
        } else {
            throw new IOException("empty url line");
        }
    }
}
