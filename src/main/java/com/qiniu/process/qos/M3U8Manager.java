package com.qiniu.process.qos;

import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.model.qdora.VideoTS;
import com.qiniu.storage.Configuration;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class M3U8Manager {

    private Client client;
    private String protocol;
    final private List<String> m3u8ContentTypes = new ArrayList<String>(){{
        add("application/x-mpegurl");
        add("application/vnd.apple.mpegurl");
    }};

    public M3U8Manager() {
        this.client = new Client();
        this.protocol = "http";
    }

    public M3U8Manager(Configuration configuration) {
        this.client = new Client(configuration);
    }

    public M3U8Manager(String protocol) {
        this.client = new Client();
        this.protocol = "https".equals(protocol)? "https" : "http";
    }

    public M3U8Manager(Configuration configuration, String protocol) {
        this.client = new Client(configuration);
        this.protocol = "https".equals(protocol)? "https" : "http";
    }

    private List<VideoTS> getVideoTSList(BufferedReader bufferedReader, String domain) throws IOException {
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

            String url = line.startsWith("http") ? line : protocol + "://" +
                    (line.startsWith("/") ? domain + line : domain + "/" + line);
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

    public List<VideoTS> getVideoTSListByUrl(String m3u8Url) throws IOException {
        Response response = client.get(m3u8Url);
        if (m3u8ContentTypes.contains(response.contentType())) {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(response.bodyStream()));
            List<VideoTS> ret = getVideoTSList(bufferedReader, m3u8Url.substring(m3u8Url.indexOf("://") + 3,
                    m3u8Url.indexOf("/", 9)));
            bufferedReader.close();
            response.close();
            return ret;
        } else {
            response.close();
            // 说明不是 m3u8 文件
            throw new IOException(m3u8Url + " 's content-type is " + response.contentType() + ", not a m3u8 type.");
        }
    }

    public List<VideoTS> getVideoTSListByFile(String domain, String m3u8FilePath) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(m3u8FilePath)));
        List<VideoTS> ret = getVideoTSList(bufferedReader, domain);
        bufferedReader.close();
        return ret;
    }
}
