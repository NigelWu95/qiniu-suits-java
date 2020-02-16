package com.qiniu.process.other;

import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.FileUtils;
import com.qiniu.util.RequestUtils;
import com.qiniu.util.StringMap;
import com.qiniu.util.URLUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class DownloadFile extends Base<Map<String, String>> {

    private String protocol;
    private String domain;
    private String urlIndex;
    private StringMap headers;
    private String suffixOrQuery;
    private boolean useQuery;
    private boolean preDown;
    private String addPrefix;
    private String rmPrefix;
    private String downPath;
    private Configuration configuration;
    private HttpDownloader downloader;

    public DownloadFile(Configuration configuration, String protocol, String domain, String urlIndex, String host, int[] range,
                        String suffixOrQuery, String addPrefix, String rmPrefix, String downPath, String savePath, int saveIndex)
            throws IOException {
        super("download", "", "", null, savePath, saveIndex);
        set(configuration, protocol, domain, urlIndex, host, range, suffixOrQuery, addPrefix, rmPrefix, downPath);
        downloader = new HttpDownloader(configuration.clone());
    }

    public DownloadFile(Configuration configuration, String protocol, String domain, String urlIndex, String host, int[] range,
                        String suffixOrQuery, String addPrefix, String rmPrefix, String downPath) throws IOException {
        super("download", "", "", null);
        set(configuration, protocol, domain, urlIndex, host, range, suffixOrQuery, addPrefix, rmPrefix, downPath);
        downloader = new HttpDownloader(configuration.clone());
    }

    public DownloadFile(Configuration configuration, String protocol, String domain, String urlIndex, String host,
                        int[] range, String suffixOrQuery, String addPrefix, String rmPrefix,
                        String downPath, String savePath) throws IOException {
        this(configuration, protocol, domain, urlIndex, host, range, suffixOrQuery, addPrefix, rmPrefix,
                downPath, savePath, 0);
    }

    private void set(Configuration configuration, String protocol, String domain, String urlIndex, String host, int[] range,
                     String suffixOrQuery, String addPrefix, String rmPrefix, String downPath) throws IOException {
        this.configuration = configuration;
        if (domain == null || "".equals(domain)) {
            if (urlIndex == null || "".equals(urlIndex)) {
                throw new IOException("please set one of domain and url-index.");
            } else {
                this.urlIndex = urlIndex;
            }
        } else {
            this.protocol = protocol == null || !protocol.matches("(http|https)") ? "http" : protocol;
            RequestUtils.lookUpFirstIpFromHost(domain);
            this.domain = domain;
            this.urlIndex = "url";
        }
        if (host != null && !"".equals(host)) {
            RequestUtils.lookUpFirstIpFromHost(host);
            headers = new StringMap().put("Host", host);
        }
        if (range != null && range.length > 0) {
            if (headers == null) headers = new StringMap();
            headers.put("Range", new StringBuilder("bytes=").append(range[0]).append("-").append(range.length > 1 ? range[1] : ""));
        }
        this.suffixOrQuery = suffixOrQuery == null ? "" : suffixOrQuery;
        useQuery = !"".equals(this.suffixOrQuery);
        this.addPrefix = addPrefix == null ? "" : addPrefix;
        this.rmPrefix = rmPrefix;
        this.downPath = downPath;
        // downPath 为空时则表示预热方式下载
        if (downPath == null || "".equals(downPath)) {
            this.preDown = true;
        } else {
            File file = new File(downPath);
            if (file.exists() && !file.isDirectory()) {
                throw new IOException("please change down-path because it's existed file.");
            }
        }
    }

    @Override
    public DownloadFile clone() throws CloneNotSupportedException {
        DownloadFile downloadFile = (DownloadFile)super.clone();
        downloadFile.downloader = new HttpDownloader(configuration.clone());
        return downloadFile;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return String.join("\t", line.get("key"), line.get(urlIndex));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws Exception {
        String url = line.get(urlIndex);
        String key = line.get("key");
        if (url == null || "".equals(url)) {
            if (key == null || "".equals(key)) throw new IOException("key is not exists or empty in " + line);
            url = String.join("", protocol, "://", domain, "/",
                    key.replace("\\?", "%3f"), suffixOrQuery);
            line.put(urlIndex, url);
            key = String.join("", addPrefix, FileUtils.rmPrefix(rmPrefix, key)); // 目标文件名
        } else {
            if (key != null) key = String.join("", addPrefix, FileUtils.rmPrefix(rmPrefix, key));
            else key = String.join("", addPrefix, FileUtils.rmPrefix(rmPrefix, URLUtils.getKey(url)));
            if (useQuery) {
                url = String.join("", url, suffixOrQuery);
                line.put(urlIndex, url);
            }
        }
        line.put("key", key);
        if (preDown) {
            downloader.download(url, headers);
            return String.join("\t", key, url);
        } else {
            String filename = String.join(FileUtils.pathSeparator, downPath, key);
            downloader.download(url, filename, headers);
            return String.join("\t", filename, url);
        }
    }

    @Override
    public void closeResource() {
        super.closeResource();
        protocol = null;
        domain = null;
        urlIndex = null;
        headers = null;
        suffixOrQuery = null;
        addPrefix = null;
        rmPrefix = null;
        configuration = null;
        downloader = null;
    }
}
