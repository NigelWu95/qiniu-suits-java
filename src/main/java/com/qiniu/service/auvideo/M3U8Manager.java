package com.qiniu.service.auvideo;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class M3U8Manager {

    public List<VideoTS> getVideoTSListByUrl(String m3u8Url) throws IOException {

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new URL(m3u8Url).openStream()));
        String rootUrl = m3u8Url.substring(0, m3u8Url.indexOf("/", 8) + 1);
        List<VideoTS> ret = getVideoTSList(bufferedReader, rootUrl);
        bufferedReader.close();

        return ret;
    }

    public List<VideoTS> getVideoTSListByFile(String rootUrl, String m3u8FilePath) throws IOException {

        FileReader fileReader = new FileReader(new File(m3u8FilePath));
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        List<VideoTS> ret = getVideoTSList(bufferedReader, rootUrl);
        bufferedReader.close();

        return ret;
    }

    public List<VideoTS> getVideoTSList(BufferedReader bufferedReader, String rootUrl) throws IOException {

        List<VideoTS> ret = new ArrayList<>();
        String line = null;
        float seconds = 0;

        while ((line = bufferedReader.readLine()) != null) {
            if (line.startsWith("#")) {
                if (line.startsWith("#EXTINF:")) {
                    line = line.substring(8, line.indexOf(","));
                    seconds = Float.parseFloat(line);
                }
                continue;
            }

            if (line.endsWith(".m3u8")) {
                List<VideoTS> tsList = getVideoTSListByUrl(rootUrl + (line.startsWith("/") ? line.substring(1) : line));
                ret.addAll(tsList);
            } else {
                ret.add(new VideoTS(rootUrl + (line.startsWith("/") ? line.substring(1) : line), seconds));
            }

            seconds = 0;
        }

        return ret;
    }
}
