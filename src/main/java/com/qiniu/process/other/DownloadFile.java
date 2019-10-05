package com.qiniu.process.other;

import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.FileUtils;
import com.qiniu.util.RequestUtils;
import com.qiniu.util.StringMap;
import com.qiniu.util.URLUtils;

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
    private Configuration configuration;
    private HttpDownloader downloader;

    public DownloadFile(Configuration configuration, String protocol, String domain, String urlIndex, String host,
                        int[] range, String suffixOrQuery, boolean preDown, String addPrefix, String rmPrefix, String savePath,
                        int saveIndex) throws IOException {
        super("download", "", "", null, savePath, saveIndex);
        set(configuration, protocol, domain, urlIndex, host, range, suffixOrQuery, preDown, addPrefix, rmPrefix);
        downloader = configuration == null ? new HttpDownloader() : new HttpDownloader(configuration);
    }

    public DownloadFile(Configuration configuration, String protocol, String domain, String urlIndex, String host,
                        int[] range, String suffixOrQuery, String downPath, String addPrefix, String rmPrefix) throws IOException {
        super("download", "", "", null);
        // 用来做只做文件 download 不记录结果的构造方法，downPath 为空时则表示预热方式下载
        if (downPath == null || "".equals(downPath)) preDown = true;
        else this.savePath = FileUtils.convertToRealPath(downPath);
        set(configuration, protocol, domain, urlIndex, host, range, suffixOrQuery, preDown, addPrefix, rmPrefix);
        downloader = configuration == null ? new HttpDownloader() : new HttpDownloader(configuration);
    }

    public DownloadFile(Configuration configuration, String protocol, String domain, String urlIndex, String host,
                        int[] range, String suffixOrQuery, boolean preDown, String addPrefix, String rmPrefix, String savePath)
            throws IOException {
        this(configuration, protocol, domain, urlIndex, host, range, suffixOrQuery, preDown, addPrefix, rmPrefix, savePath,
                0);
    }

    private void set(Configuration configuration, String protocol, String domain, String urlIndex, String host,
                     int[] range, String suffixOrQuery, boolean preDown, String addPrefix, String rmPrefix) throws IOException {
        this.configuration = configuration;
        if (urlIndex == null || "".equals(urlIndex)) {
            this.urlIndex = "url";
            if (domain == null || "".equals(domain)) {
                throw new IOException("please set one of domain and url-index.");
            } else {
                this.protocol = protocol == null || !protocol.matches("(http|https)") ? "http" : protocol;
                RequestUtils.lookUpFirstIpFromHost(domain);
                this.domain = domain;
            }
        } else {
            this.urlIndex = urlIndex;
        }
        if (host != null && !"".equals(host)) {
            RequestUtils.lookUpFirstIpFromHost(host);
            headers = new StringMap().put("Host", host);
        }
        if (range != null && range.length > 0) {
            if (headers == null) headers = new StringMap();
            headers.put("Range", range[0] + "-" + (range.length > 1 ? range[1] : ""));
        }
        this.suffixOrQuery = suffixOrQuery == null ? "" : suffixOrQuery;
        useQuery = !"".equals(this.suffixOrQuery);
        this.preDown = preDown;
        this.addPrefix = addPrefix == null ? "" : addPrefix;
        this.rmPrefix = rmPrefix;
    }

    public DownloadFile clone() throws CloneNotSupportedException {
        DownloadFile downloadFile = (DownloadFile)super.clone();
        downloadFile.downloader = configuration == null ? new HttpDownloader() : new HttpDownloader(configuration);
        return downloadFile;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get("key") + "\t" + line.get(urlIndex);
    }

    @Override
    protected String singleResult(Map<String, String> line) throws Exception {
        String url = line.get(urlIndex);
        String key = line.get("key");
        if (url == null || "".equals(url)) {
            if (key == null || "".equals(key)) throw new IOException("key is not exists or empty in " + line);
            url = protocol + "://" + domain + "/" + key.replace("\\?", "%3f") + suffixOrQuery;
            line.put(urlIndex, url);
            key = addPrefix + FileUtils.rmPrefix(rmPrefix, key); // 目标文件名
        } else {
            if (key != null) key = addPrefix + FileUtils.rmPrefix(rmPrefix, key);
            else key = addPrefix + FileUtils.rmPrefix(rmPrefix, URLUtils.getKey(url));
            if (useQuery) {
                url = url + suffixOrQuery;
                line.put(urlIndex, url);
            }
        }
        line.put("key", key);
        if (preDown) {
            downloader.download(url, headers);
            return key + "\t" + url;
        } else {
            String filename = (fileSaveMapper == null ? savePath : fileSaveMapper.getSavePath()) + FileUtils.pathSeparator + key;
            downloader.download(url, filename, headers);
            return key + "\t" + url + "\t" + filename;
        }
    }

    @Override
    protected void parseSingleResult(Map<String, String> line, String result) throws IOException {
        if (preDown) fileSaveMapper.writeSuccess(result, false);
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
