package com.qiniu.service.media;

import com.qiniu.common.FileMap;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.URL;
import java.util.List;

public class M3U8Tools {

    private FileMap fileMap;

    public M3U8Tools(FileMap fileMap) {
        this.fileMap = fileMap;
    }

    public void merge(List<VideoTS> tsList, String url, String targetFileDir) throws FileNotFoundException {
        File fileDir = new File(targetFileDir);
        InputStream inputStream = null;
        File file = new File(fileDir, url.substring(url.indexOf("/", 8) + 1, url.lastIndexOf(".")));
        FileOutputStream fileOutputStream = new FileOutputStream(file);

        for (VideoTS ts : tsList) {
            try {
                inputStream = new URL(ts.getUrl()).openStream();
                IOUtils.copyLarge(inputStream, fileOutputStream);
                fileMap.writeSuccess(ts.getUrl());
            } catch (IOException e) {
                fileMap.writeErrorOrNull(ts.getUrl() + "\t" + e.toString());
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

    public void download(List<VideoTS> tsList, final String targetFileDir) {
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
                fileMap.writeSuccess(ts.toString());
            } catch (IOException e) {
                fileMap.writeErrorOrNull(ts + "\t" + e.toString());
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
