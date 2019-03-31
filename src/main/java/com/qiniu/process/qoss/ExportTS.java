package com.qiniu.process.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.model.qdora.VideoTS;
import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.FileNameUtils;
import com.qiniu.util.RequestUtils;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

public class ExportTS extends Base {

    private M3U8Manager m3U8Manager;
    private String domain;
    private String protocol;
    private String urlIndex;

    public ExportTS(Configuration configuration, String domain, String protocol, String urlIndex, String rmPrefix,
                    String savePath, int saveIndex) throws IOException {
        super("exportts", "", "", configuration, null, rmPrefix, savePath, saveIndex);
        set(domain, protocol, urlIndex);
        this.m3U8Manager = new M3U8Manager(configuration.clone(), protocol);
    }

    public void updateExport(String domain, String protocol, String urlIndex, String rmPrefix)
            throws IOException {
        set(domain, protocol, urlIndex);
        this.rmPrefix = rmPrefix;
    }

    private void set(String domain, String protocol, String urlIndex) throws IOException {
        if (urlIndex == null || "".equals(urlIndex)) {
            this.urlIndex = null;
            if (domain == null || "".equals(domain)) {
                throw new IOException("please set one of domain and urlIndex.");
            } else {
                RequestUtils.checkHost(domain);
                this.domain = domain;
                this.protocol = protocol == null || !protocol.matches("(http|https)") ? "http" : protocol;
            }
        } else {
            this.urlIndex = urlIndex;
        }
    }

    public ExportTS(Configuration configuration, String domain, String protocol, String urlIndex, String rmPrefix,
                    String savePath) throws IOException {
        this(configuration, domain, protocol, urlIndex, rmPrefix, savePath, 0);
    }

    public ExportTS clone() throws CloneNotSupportedException {
        ExportTS exportTS = (ExportTS)super.clone();
        exportTS.m3U8Manager = new M3U8Manager(configuration.clone(), protocol);
        return exportTS;
    }

    @Override
    protected Map<String, String> formatLine(Map<String, String> line) throws IOException {
        if (urlIndex == null) {
            line.put("key", FileNameUtils.rmPrefix(rmPrefix, line.get("key")));
            urlIndex = "url";
            line.put(urlIndex, protocol + "://" + domain + "/" + line.get("key").replaceAll("\\?", "%3F"));
        }
        return line;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get(urlIndex);
    }

    @Override
    protected String singleResult(Map<String, String> line) throws QiniuException {
        try {
            return String.join("\n", m3U8Manager.getVideoTSListByUrl(line.get(urlIndex))
                    .stream().map(VideoTS::toString).collect(Collectors.toList()));
        } catch (IOException e) {
            throw new QiniuException(e);
        }
    }
}
