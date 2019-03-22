package com.qiniu.util;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class M3U8Utils {

    public static class VideoTS {

        private String url;
        private float seconds;

        public VideoTS(String url, float seconds) {
            this.url = url;
            this.seconds = seconds;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public float getSeconds() {
            return seconds;
        }

        public void setSeconds(float seconds) {
            this.seconds = seconds;
        }

        public String toString() {
            return url + ": " + seconds + "sec";
        }

    }

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

    public static List<VideoTS> getVideoTSListByUrl(String m3u8Url) throws IOException {

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new URL(m3u8Url).openStream()));
        String rootUrl = m3u8Url.substring(0, m3u8Url.indexOf("/", 8) + 1);
        List<VideoTS> ret = getVideoTSList(bufferedReader, rootUrl);
        bufferedReader.close();

        return ret;
    }

    public static List<VideoTS> getVideoTSListByFile(String rootUrl, String m3u8FilePath) throws IOException {

        FileReader fileReader = new FileReader(new File(m3u8FilePath));
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        List<VideoTS> ret = getVideoTSList(bufferedReader, rootUrl);
        bufferedReader.close();

        return ret;
    }

    public static List<VideoTS> getVideoTSList(BufferedReader bufferedReader, String rootUrl) throws IOException {

        List<VideoTS> ret = new ArrayList<>();
        String line;
        float seconds = 0;

        while ((line = bufferedReader.readLine()) != null) {
            if (line.startsWith("#")) {
                if (line.startsWith("#EXTINF:")) {
                    line = line.substring(8, line.indexOf(","));
                    seconds = Float.parseFloat(line);
                }
                continue;
            }

            String url = line.startsWith("http") ? line : line.startsWith("/") ? rootUrl + line.substring(1) :
                    rootUrl + line;
            if (line.endsWith(".m3u8")) {
                List<VideoTS> tsList = getVideoTSListByUrl(url);
                ret.addAll(tsList);
            } else {
                ret.add(new VideoTS(url, seconds));
            }

            seconds = 0;
        }

        return ret;
    }
}
