package com.qiniu.util;

import com.qiniu.model.qdora.VideoTS;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public final class M3U8Utils {

    public static void merge(List<VideoTS> tsList, String url, String targetFileDir) throws IOException {
        File fileDir = new File(targetFileDir);
        InputStream inputStream = null;
        File file = new File(fileDir, url.substring(url.indexOf("/", 8) + 1, url.lastIndexOf(".")));
        FileOutputStream fileOutputStream = new FileOutputStream(file);

        for (VideoTS ts : tsList) {
            try {
                inputStream = new URL(ts.getUrl()).openStream();
                IOUtils.copyLarge(inputStream, fileOutputStream);
                System.out.println(ts.getUrl());
            } catch (IOException e) {
                System.out.println(ts.getUrl() + "\t" + e.getMessage());
            }
        }

        try {
            fileOutputStream.close();
        } catch (IOException e) {
            fileOutputStream = null;
        }
        try {
            inputStream.close();
        } catch (IOException e) {
            inputStream = null;
        }
    }

    public static void download(List<VideoTS> tsList, final String targetFileDir) throws IOException {
        File dir = new File(targetFileDir).getParentFile();
        String url;
        File file;
        FileOutputStream fileOutputStream = null;
        InputStream inputStream = null;

        if (!dir.exists()) {
            dir.mkdirs();
        }

        for (VideoTS ts : tsList) {
            url = ts.getUrl();

            try {
                file = new File(dir, url.substring(url.indexOf("/", 8) + 1));
                fileOutputStream = new FileOutputStream(file);
                inputStream = new URL(url).openStream();
                IOUtils.copyLarge(inputStream, fileOutputStream);
                System.out.println(ts.toString());
            } catch (IOException e) {
                System.out.println(ts + "\t" + e.getMessage());
            }
        }

        try {
            fileOutputStream.close();
        } catch (IOException e) {
            fileOutputStream = null;
        }
        try {
            inputStream.close();
        } catch (IOException e) {
            inputStream = null;
        }
    }
}
