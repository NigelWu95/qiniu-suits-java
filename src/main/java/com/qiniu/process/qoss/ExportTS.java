package com.qiniu.process.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.model.qdora.VideoTS;
import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.RequestUtils;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

public class ExportTS extends Base<Map<String, String>> {

    private String domain;
    private String protocol;
    private String urlIndex;
    private M3U8Manager m3U8Manager;

    public ExportTS(Configuration configuration, String domain, String protocol, String urlIndex, String savePath,
                    int saveIndex) throws IOException {
        super("exportts", "", "", configuration, null, savePath, saveIndex);
        set(domain, protocol, urlIndex);
        this.m3U8Manager = new M3U8Manager(configuration.clone(), protocol);
    }

    public void updateExport(String domain, String protocol, String urlIndex)
            throws IOException {
        set(domain, protocol, urlIndex);
        this.m3U8Manager = new M3U8Manager(configuration.clone(), protocol);
    }

    private void set(String domain, String protocol, String urlIndex) throws IOException {
        if (urlIndex == null || "".equals(urlIndex)) {
            this.urlIndex = "url";
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

    public ExportTS(Configuration configuration, String domain, String protocol, String urlIndex, String savePath)
            throws IOException {
        this(configuration, domain, protocol, urlIndex, savePath, 0);
    }

    public ExportTS clone() throws CloneNotSupportedException {
        ExportTS exportTS = (ExportTS)super.clone();
        exportTS.m3U8Manager = new M3U8Manager(configuration.clone(), protocol);
        return exportTS;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get(urlIndex);
    }

    @Override
    protected boolean validCheck(Map<String, String> line) {
        return line.get("key") != null;
    }

    @Override
    protected void parseSingleResult(Map<String, String> line, String result) throws IOException {
        fileSaveMapper.writeSuccess(result, false);
    }

    @Override
    protected String singleResult(Map<String, String> line) throws QiniuException {
        String url = line.get(urlIndex);
        try {
            if (url == null || "".equals(url)) url =  protocol + "://" + domain + "/" + line.get("key").replaceAll("\\?", "%3F");
            return String.join("\n", m3U8Manager.getVideoTSListByUrl(url).stream().map(VideoTS::toString).collect(Collectors.toList()));
        } catch (QiniuException e) {
            throw e;
        } catch (IOException e) {
            throw new QiniuException(e, e.getMessage());
        }
    }
}
